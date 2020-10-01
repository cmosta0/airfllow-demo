from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow import DAG
from airflow.utils.dates import days_ago

default_args = {'start_date': days_ago(0, 0, 10), 'owner': 'cm0'}
abc_dag = DAG(dag_id="poc_abc_dag", default_args=default_args, schedule_interval=None)

a = DummyOperator(task_id="operator_a", dag=abc_dag)
b = DummyOperator(task_id="operator_b", dag=abc_dag)
c = DummyOperator(task_id="operator_c", dag=abc_dag)
d = DummyOperator(task_id="operator_d", dag=abc_dag)
e = DummyOperator(task_id="operator_e", dag=abc_dag)

bash_a = BashOperator(task_id="bash_date", dag=abc_dag, bash_command="echo date")
bash_b = BashOperator(task_id="bash_echo", dag=abc_dag, bash_command="echo 'hello guanatos'")
bash_c = BashOperator(task_id="bash_command", dag=abc_dag, bash_command="spark-submit --version")

a >> [b, c, d] >> e >> [bash_b, bash_c]
c >> bash_a

