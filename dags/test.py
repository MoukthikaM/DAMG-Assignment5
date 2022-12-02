# from snowflake.snowpark.session import Session

from airflow.decorators import dag, task
# from airflow_tasks import bulk_elt,bulk_train_predict_task
import pendulum
import json
default_args = {
    'owner': 'moukthika',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# with open('creds.json') as f:
#     connection_parameters = json.load(f)  
    
# session = Session.builder.configs(connection_parameters).create()

# snowpark_version = VERSION
# print('Database                    : {}'.format(session.get_current_database()))
# print('Schema                      : {}'.format(session.get_current_schema()))
# print('Warehouse                   : {}'.format(session.get_current_warehouse()))
# print('Role                        : {}'.format(session.get_current_role()))
# print('Snowpark for Python version : {}.{}.{}'.format(snowpark_version[0],snowpark_version[1],snowpark_version[2]))


# session=[]
@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['test'])
def test_setup_taskflow():
    import uuid
    import json
    #Task order - one-time setup
    # bulk_elt(session)
    # bulk_train_predict_task(session)
    @task()
    def extract():
        """
        #### Extract task
        A simple "extract" task to get data ready for the rest of the
        pipeline. In this case, getting data is simulated by reading from a
        hardcoded JSON string.
        """
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'

        order_data_dict = json.loads(data_string)
        return order_data_dict

    @task()
    def load(data):
         print(data)
    order_data = extract()
    load(order_data)



test_setup_taskflow()

