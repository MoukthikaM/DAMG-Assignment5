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
def bulk_elt(state_dict:dict, 
             use_prestaged=False) -> dict:
    print("from bulk ELT")
    from snowflake.snowpark.session import Session
    
    import json
    with open('./include/creds.json') as f:
        connection_parameters = json.load(f)  
    session = Session.builder.configs(connection_parameters).create()
    import requests
    import pandas as pd
    from dags.mlops_pipeline import extract_to_stage,load,transform
    # state_dict['connection_parameters']['database'] = 'CUSTOMERS'
    # state_dict['connection_parameters']['schema'] = 'PUBLIC'
    state_dict['load_stage_name']='LOAD_STAGE' 
    state_dict['download_base_url']='https://s3.amazonaws.com/damgbucket/data/'
    state_dict['trips_table_name']='CUSTOMERS'
    state_dict['load_table_name'] = 'RAW_'

    
    
    files_to_load = extract_to_stage(session=session, 
                                download_base_url=state_dict['download_base_url'], 
                                load_stage_name=state_dict['load_stage_name'])
    
    stage_table_names = load(session=session, 
                          files_to_load=files_to_load, 
                          load_stage_names=state_dict['load_stage_name'], 
                          load_table_name=state_dict['load_table_name'])

    
    trips_table_name = transform(session=session,  
                               trips_table_name=state_dict['trips_table_name'])
    
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
    






