def inc_extract_to_stage(session, schema1_files_to_download: list,schema2_download_files: list, download_base_url: str, load_stage_name:str):
    
    import requests
    from zipfile import ZipFile
    from io import BytesIO
    import os

    schema1_load_stage = load_stage_name+'/schema1/'
    schema1_files_to_load = list()

    for file_name in schema1_files_to_download:
        url1 = download_base_url+file_name
        print('Downloading and unzipping: '+url1)

        r = requests.get(url1)
        file = ZipFile(BytesIO(r.content))
        csv_file_name=file.namelist()[0]
        schema1_files_to_load.append(csv_file_name)
        file.extract(csv_file_name)
        file.close()
        
    schema2_load_stage = load_stage_name+'/schema2/'
    schema2_files_to_load = list()
    
    for file_name in schema2_download_files:

        url2 = download_base_url+file_name

        print('Downloading and unzipping: '+url2)
        r = requests.get(url2)
        file = ZipFile(BytesIO(r.content))
        csv_file_name=file.namelist()[0]
        schema2_files_to_load.append(csv_file_name)
        file.extract(csv_file_name)
        file.close()
        
    load_stage_names = {'schema1' : schema1_load_stage, 'schema2' : schema2_load_stage}
    files_to_load = {'schema1': schema1_files_to_load, 'schema2': schema2_files_to_load}

    return load_stage_names, files_to_load

def load(session, files_to_load:dict, load_stage_names:dict, load_table_name:str):
    import pandas as pd
    user_details_1 = pd.read_csv(files_to_load['schema1'][0])
    user_details_2 = pd.read_csv(files_to_load['schema2'][0])
    user_activity_1 = pd.read_csv(files_to_load['schema1'][1])
    user_activity_2 = pd.read_csv(files_to_load['schema2'][1])
    
    # Create a pandas data frame from the Snowflake table
    UD_df = session.table('user_details_new').toPandas()
    user_details=pd.concat([UD_df,user_details_2])
    print(f"'UD_df' local dataframe created. Number of records: {len(UD_df)} ")
    snowdf_activity = session.createDataFrame(user_details)
    snowdf_activity.write.mode("overwrite").saveAsTable("user_details_new") 
    session.table("user_details_new").limit(5).show(5)
    
    # Create a pandas data frame from the Snowflake table
    UA_df = session.table('user_activity_new').toPandas()
    user_activity=pd.concat([UA_df,user_activity_2])
    print(f"'UA_df' local dataframe created. Number of records: {len(UA_df)} ")
    snowdf_activity = session.createDataFrame(user_activity)
    snowdf_activity.write.mode("overwrite").saveAsTable("user_activity_new") 
    session.table("user_activity_new").limit(5).show(5)

    return load_table_name