from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
import os
import logging

DAG_ID = 'poc_word_count_dag'
LOGGER = logging.getLogger(DAG_ID)
logging.basicConfig(level=logging.INFO)


def say_hello():
    """
    used for python operator
    :return: None
    """
    LOGGER.info('hello, meetup audience!')


def get_current_parent_folder():
    """
    Used to compute absolute path for input files.

    :return: parent folder for <this> file
    """
    parent_script_folder = os.path.dirname(os.path.realpath(__file__))
    return parent_script_folder


def get_counters_subdag(parent_dag, absolute_script_path):
    """
    Factory function to create a new subdag given parent_dag values

    :param parent_dag: Main dag
    :return: created subdag
    """
    counter_subdag = DAG(
        dag_id=f"{parent_dag.dag_id}.counters",
        default_args=parent_dag.default_args,
        start_date=parent_dag.start_date
    )

    simple_bash_words_counter = BashOperator(
        task_id=f"{counter_subdag.dag_id}.bash_words_counter",
        dag=counter_subdag,
        bash_command=f"python {absolute_script_path}"
    )

    bash_spark_submit_words_counter = BashOperator(
        task_id=f"{counter_subdag.dag_id}.bash_spark_submit_words_counter",
        dag=counter_subdag,
        bash_command=f"spark-submit {absolute_script_path}"
    )

    words_counter_spark_operator = SparkSubmitOperator(
        dag=counter_subdag,
        application=absolute_script_path,
        task_id=f"{counter_subdag.dag_id}.spark_word_counter"
    )

    simple_bash_words_counter >> [words_counter_spark_operator, bash_spark_submit_words_counter]

    return counter_subdag


# Min required data for our dag
args = {'owner': 'cm0', 'start_date': days_ago(2)}

# Main dag creation
WORD_COUNT_DAG = DAG(dag_id=DAG_ID, schedule_interval=None, default_args=args)

# Application (script to be executed by Airflow)
script_path = get_current_parent_folder() + "/../../resources/pyspark/read_random_words.py"


hello_function = PythonOperator(
    default_args=args,
    dag=WORD_COUNT_DAG,
    python_callable=say_hello,
    task_id="hello_function"
)

# More about macros: https://airflow.apache.org/docs/stable/macros-ref.html
execution_ds = BashOperator(task_id="print_execution_ds", dag=WORD_COUNT_DAG, bash_command="echo '{{ ds_nodash }}'")

start = DummyOperator(task_id="start", dag=WORD_COUNT_DAG)
end = DummyOperator(task_id="end", dag=WORD_COUNT_DAG)

# Create a SubDagOperator with the return value from get_counters_subdag and use it on main dag
counters_subdag = SubDagOperator(
    task_id=f"counters",
    subdag=get_counters_subdag(WORD_COUNT_DAG, script_path),
    dag=WORD_COUNT_DAG
)


# Order/Dependencies...
start >> execution_ds >> hello_function >> counters_subdag >> end

