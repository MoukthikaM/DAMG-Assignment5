from airflow.decorators import task
@task.virtualenv(python_version=3.8)
def reset_database(state_dict:dict, prestaged=False)->dict:
    from snowflake.snowpark.session import Session
    from dags.mlops_pipeline import database
    import json
    state_dict['connection_parameters']={}
    state_dict['connection_parameters']['database'] = 'CUSTOMERS'
    state_dict['connection_parameters']['schema'] = 'PUBLIC'
    state_dict['load_stage_name']='LOAD_STAGE' 
    state_dict['download_base_url']='https://s3.amazonaws.com/damgbucket/data/'
    state_dict['trips_table_name']='ecommerce'
    state_dict['load_table_name'] = 'RAW_'
    with open('./include/creds.json') as f:
        connection_parameters = json.load(f)  
    session = Session.builder.configs(connection_parameters).create()
    database(session, state_dict)
    
    return state_dict



@task.virtualenv(python_version=3.8)
def incremental_elt(state_dict:dict) -> dict:
    
    print("from bulk ELT")
    from snowflake.snowpark.session import Session
    
    import json
    with open('./include/creds.json') as f:
        connection_parameters = json.load(f)  
    session = Session.builder.configs(connection_parameters).create()
    import requests
    import pandas as pd
    from dags.mlops_pipeline_incremental import inc_extract_to_stage,load
    from dags.mlops_pipeline import transform
    # state_dict['connection_parameters']['database'] = 'CUSTOMERS'
    # state_dict['connection_parameters']['schema'] = 'PUBLIC'
    state_dict['load_stage_name']='LOAD_STAGE' 
    state_dict['download_base_url']='https://s3.amazonaws.com/damgbucket/data/'
    state_dict['trips_table_name']='CUSTOMERS'
    state_dict['load_table_name'] = 'RAW_'
    import pandas as pd

    file_name1 = 'UserDetails.csv.zip'
    file_name2 = 'UserActivity.csv.zip'
    file_names = [file_name1,file_name2]
    schema1_files_to_download = file_names
    schema1_files_to_download
    from datetime import datetime

    file_name_end1 = '-UserDetails.csv.zip'
    file_name_end2 = '-UserActivity.csv.zip'
    date_range = pd.period_range(start=datetime.strptime("202202", "%Y%m"), 
                             end=datetime.now(), 
                             freq='M').strftime("%Y%m")
    daterange=date_range.to_list()
    length=len(daterange)
    schema2_files_to_download_1 = [daterange[length-1]+file_name_end1 and daterange[length-1]+file_name_end1]
    schema2_files_to_download_2 = [daterange[length-1]+file_name_end1 and daterange[length-1]+file_name_end2 ]
    schema2_files_to_download = schema2_files_to_download_1 + schema2_files_to_download_2
    schema2_files_to_download
    load_stage_names,files_to_load=inc_extract_to_stage(session, schema1_files_to_download, schema2_files_to_download, state_dict['download_base_url'], state_dict['load_stage_name'])
    
    load_table_name=load(session, files_to_load, load_stage_names, state_dict['load_table_name'])
    
    table_name = transform(session,state_dict['trips_table_name'])
    
    session.close()
    return state_dict



@task.virtualenv(python_version=3.8)
def bulk_train_predict_task(state_dict:dict)-> dict: 
    print("BYE from train predict")
    
    from snowflake.snowpark.session import Session
    from dags.mlops_pipeline import predict
    import json
    with open('./include/creds.json') as f:
        connection_parameters = json.load(f)
    session = Session.builder.configs(connection_parameters).create()  
    output=predict(session)
    session.close()
    return state_dict
    

