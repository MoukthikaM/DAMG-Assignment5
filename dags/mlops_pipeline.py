def extract_to_stage(session, download_base_url: str, load_stage_name:str):
    
    import requests
    from zipfile import ZipFile
    from io import BytesIO
    import os

    schema1_load_stage = load_stage_name+'/schema1/'
    
    file_name1 = 'UserDetails.csv.zip'
    file_name2 = 'UserActivity.csv.zip'
    files_to_download = [file_name1]
    schema1_download_files = list(files_to_download)
    for file_name in schema1_download_files:
        url1 = download_base_url+file_name
        print('Downloading and unzipping: '+url1)

        r = requests.get(url1)
        file = ZipFile(BytesIO(r.content))
        csv_file_name=file.namelist()[0]
        file.extract(csv_file_name)
        file.close()
        
    schema2_load_stage = load_stage_name+'/schema2/'
    files_to_download = [file_name2]
    schema2_download_files = list(files_to_download)
    for file_name in schema2_download_files:

        url2 = download_base_url+file_name

        print('Downloading and unzipping: '+url2)
        r = requests.get(url2)
        print(r)
        file = ZipFile(BytesIO(r.content))
        csv_file_name=file.namelist()[0]
        file.extract(csv_file_name)
        file.close()
    files_to_load = {'schema1': schema1_download_files, 'schema2': schema2_download_files}
    return files_to_load

def load(session, files_to_load:dict, load_stage_names:dict, load_table_name:str):
    import pandas as pd
    
    user_details = pd.read_csv("UserDetails.csv")
    user_details.head()
    
    # Create a Snowpark DF from the pandas DF
    snowdf_details = session.createDataFrame(user_details)
    snowdf_details.show()
    
    # Loading user details data from Snowpark DF to a Snowflake internal table
    snowdf_details.write.mode("overwrite").saveAsTable("user_details_new") 
    session.table("user_details_new").show(5)
    
    user_activity = pd.read_csv("UserActivity.csv")
    user_activity.head()
    
    # Create a Snowpark DF from the pandas DF
    snowdf_activity = session.createDataFrame(user_activity)
    snowdf_activity.show()
    
    # Loading user activity data from Snowpark DF to a Snowflake internal table
    snowdf_activity.write.mode("overwrite").saveAsTable("user_activity_new") 
    session.table("user_activity_new").limit(5).show(5)
    
    # Create a pandas data frame from the Snowflake table
    UD_df = session.table('user_details_new').toPandas() 
    print(f"'UD_df' local dataframe created. Number of records: {len(UD_df)} ")
    
    # Create a pandas data frame from the Snowflake table
    UA_df = session.table('user_activity_new').toPandas() 
    print(f"'UA_df' local dataframe created. Number of records: {len(UA_df)} ")

    return load_table_name




def transform(session, trips_table_name:str):
    import pandas as pd
    
    UD_df = session.table('user_details_new').toPandas()
    UA_df = session.table('user_activity_new').toPandas() 
    
    transformed_df = pd.concat([UD_df, UA_df], axis=1, join='inner')
    # display(transformed_df)

    customers = pd.DataFrame(transformed_df)
    customers.to_csv('customers.csv')
    customers = pd.read_csv('customers.csv')

    snowdf_cust = session.createDataFrame(customers)
    snowdf_cust.drop("Unnamed: 0","Email.1").show()
    snowdf_cust.drop("Unnamed: 0","Email.1").write.mode("overwrite").saveAsTable("customers") 

    customers = session.table('customers').toPandas() 

    print(f"'customers' local dataframe created. Number of records: {len(customers)} ")
    
    return trips_table_name






def predict(session):
    from snowflake.snowpark import functions as F
    from snowflake.snowpark.types import FloatType
    from snowflake.snowpark.version import VERSION
    import pandas as pd 
    from sklearn.model_selection import train_test_split
    from sklearn.linear_model import LinearRegression
    import json
    custdf = session.table("customers")
    custdf=custdf.to_pandas()
    # Define X and Y for modeling
    X = custdf[['Avg. Session Length', 'Time on App',
       'Time on Website', 'Length of Membership']]
    Y = custdf['Yearly Amount Spent']
    # Split into training & Testing datasets
    X_train, X_test, y_train, y_test = train_test_split(X, Y,
                                 test_size=0.3, random_state=101)
    # Create an instance of Linear Regression and Fit the training datasets
    lm = LinearRegression()
    lm.fit(X_train,y_train)
    # Creating a User Defined Function within Snowflake to do the scoring there
    def predict_pandas_udf(df: pd.DataFrame) -> pd.Series:
        import pandas as pd
        return pd.Series(lm.predict(df))
    from snowflake.snowpark.functions import pandas_udf
    linear_model_vec = pandas_udf(func=predict_pandas_udf,
                                return_type=FloatType(),
                                input_types=[FloatType(),FloatType(),FloatType(),FloatType()],
                                session=session,
                                packages = ("pandas","scikit-learn"), max_batch_size=200)
    
    # Calling the UDF to do the scoring (pushing down to Snowflake)
    output = session.table('CUSTOMERS').select(*list(X.columns),
                    linear_model_vec(list(X.columns)).alias('PREDICTED_SPEND'), 
                    (F.col('Yearly Amount Spent')).alias('ACTUAL_SPEND')
                    )

    output.show(5)
    # Save the predicted output as a table on Snowflake
    ###used later in Streamlit app
    output.write.mode("overwrite").saveAsTable("PREDICTED_CUSTOMER_SPEND") 

    # Also get a local dataframe to review the results
    output=output.toPandas()
    print(output)

    return output
    
    

def database(session, state_dict:dict, prestaged=False):
    
    _ = session.sql('CREATE OR REPLACE DATABASE '+state_dict['connection_parameters']['database']).collect()
    _ = session.sql('CREATE SCHEMA IF NOT EXISTS '+state_dict['connection_parameters']['schema']).collect() 

    if prestaged:
        sql_cmd = 'CREATE OR REPLACE STAGE '+state_dict['load_stage_name']+\
                  ' url='+state_dict['connection_parameters']['download_base_url']
        _ = session.sql(sql_cmd).collect()
    else: 
        _ = session.sql('CREATE STAGE IF NOT EXISTS '+state_dict['load_stage_name']).collect()  
