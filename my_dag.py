import datetime
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from random import  randint


def hello():
    print("Airflow")


def sum_of_numbers():
    print(randint(0, 100) + randint(0, 100))


def numbers_to_log():
    with open('dags/output.txt', 'a') as f:
        f.writelines(' '.join(
            [str(randint(0, 100)), str(randint(0, 100))]
        ) + '\n')


def difference_to_end():
    with open('dags/output.txt', 'r') as f:
        string = f.readline()
        sum1 = sum([int(i) for i in string.split(' ')])
        string = f.readline()
        sum2 = sum([int(i) for i in string.split(' ')])

    with open('dags/output.txt', 'a') as f:
        f.writelines(str(sum1 - sum2) + '\n')


def difference_refresh_the_end():
    with open('dags/output.txt', 'r') as f:
        lines = f.readlines()
        string = lines[0]
        sum1 = sum([int(i) for i in string.split(' ')])
        string = lines[1]
        sum2 = sum([int(i) for i in string.split(' ')])

    with open('dags/output.txt', 'w') as f:
        lines[-1] = str(sum1 - sum2) + '\n'
        f.writelines(lines)


with DAG(
    dag_id="first_dag",
    schedule="* * * * *",
    catchup=False,
    start_date=pendulum.datetime(2022, 12, 24, 12, 10, 0, tz="UTC"),
    end_date=pendulum.datetime(2022, 12, 24, 12, 16, 0, tz="UTC"),
    tags=["my"],
) as dag:
    bash_task = BashOperator(
        task_id="hello",
        bash_command="echo hello",
    )

    python_task = PythonOperator(
        task_id="world",
        python_callable=hello
    )

    sum_of_numbers = PythonOperator(
        task_id="sum_of_numbers",
        python_callable=sum_of_numbers
    )

    numbers_to_log = PythonOperator(
        task_id="numbers_to_log",
        python_callable=numbers_to_log
    )

    difference_to_end = PythonOperator(
        task_id="difference_to_end",
        python_callable=difference_to_end
    )

    difference_refresh_the_end = PythonOperator(
        task_id="difference_refresh_the_end",
        python_callable=difference_refresh_the_end
    )

    bash_task >> python_task >> sum_of_numbers >> numbers_to_log >> difference_to_end >> difference_refresh_the_end
