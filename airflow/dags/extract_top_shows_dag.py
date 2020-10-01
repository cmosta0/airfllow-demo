from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable
from datadog import initialize
from datadog import api
import os
import logging

DAG_ID = "poc_extract_shows_dag"

LOGGER = logging.getLogger(DAG_ID)
logging.basicConfig(level=logging.INFO)


class DatadogClient:
    """
    DatadogClient provides methods to be sent to airflow callbacks for sending events
    to datadog.

    :param `dag_name`: Name for dag also used in datadog tags.
    """

    def __init__(self, aiflow_dag_name):
        logging.info("Reading DD config - using airflow variables")
        # Uncomment below 2 lines and remove quotes once you have defined your airflow-variables
        dd_apikey = "" # Variable.get("SECRET_DD_API_KEY")
        dd_appkey = "" # Variable.get("SECRET_DD_APP_KEY")
        logging.info("Initializing DD client")
        initialize(api_key=dd_apikey, app_key=dd_appkey)
        if aiflow_dag_name is None:
            raise ValueError("aiflow_dag_name is not defined")

        self.tags = ["application:airflow-meetup-poc",f"process:{aiflow_dag_name}"]

    def post_error_event(self, context):
        event_title = f"Heeey!, something went wrong with dag: {context['dag']}; failed task: {context['task']}"
        event_description = f"Failed instance:{context['task_instance']} - timestamp: {context['ts']}"
        api_response = api.Event.create(title=event_title, text=event_description, tags=self.tags, alert_type="error")
        LOGGER.info(f"DD api response: {api_response}")

    def post_success_event(self, context):
        event_title = f"Dag: {context['dag']} update, task successfully completed: {context['task']}"
        event_description = f"Instance:{context['task_instance']}- timestamp: {context['ts']}"
        api_response = api.Event.create(title=event_title, text=event_description, tags=self.tags, alert_type="success")
        LOGGER.info(f"DD api response: {api_response}")


def get_current_parent_folder():
    """
    Used to compute absolute path for input files.

    :return: parent folder for <this> file
    """
    parent_script_folder = os.path.dirname(os.path.realpath(__file__))
    return parent_script_folder


DD_CLIENT = DatadogClient(DAG_ID)
def_args = {
    'start_date': days_ago(1),
    'on_failure_callback': DD_CLIENT.post_error_event,
    'on_success_callback': DD_CLIENT.post_success_event,
    'owner': 'cm0'
}

with DAG(dag_id=DAG_ID,
         schedule_interval=None,
         default_args=def_args) as dag:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    spark_app_folder = get_current_parent_folder() + "/../../resources/pyspark/"

    extract_top_netflix_shows = SparkSubmitOperator(
        task_id="extract_top_netflix_shows",
        application=spark_app_folder + "top_netflix_shows_identification.py",
        application_args=["top_netflix_shows", "10"]
    )

    prime_top_prime_shows = SparkSubmitOperator(
        task_id="prime_top_prime_shows",
        application=spark_app_folder + "top_prime_shows_identification.py",
        application_args=["top_prime_shows", "15"]
    )

    unify_shows = SparkSubmitOperator(
        task_id="unify_shows",
        application=spark_app_folder + "shows_unification.py",
        application_args=["top_netflix_shows","top_prime_shows","unified_shows"]
    )

    start >> [extract_top_netflix_shows, prime_top_prime_shows] >> unify_shows >> end
