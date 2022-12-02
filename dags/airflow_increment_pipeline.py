from airflow.operators.python import PythonVirtualenvOperator
from airflow_tasks import bulk_train_predict_task,reset_database
from airflow.decorators import dag, task
from airflow_incremental_tasks import incremental_elt
import pendulum
import json
default_args = {
    'owner': 'mouk',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}



# session=[]
@dag(
    schedule='0 0 1 * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['customerspend_incremental_setup_taskflow'])
def customerspend_incremental_setup_taskflow():
    import uuid
    import json
    #Task order - one-time setup
    with open('./include/creds.json') as f:
        connection_parameters = json.load(f)
    state_dict=connection_parameters
    # state_dict=reset_database(state_dict, prestaged=False)
    state_dict=incremental_elt(state_dict)
    state_dict=bulk_train_predict_task(state_dict)  
customerspend_incremental_setup_taskflow()



