from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import time

def my_python_function(task_id):
    print(f"Executing {task_id}")
    time.sleep(15) 
    print(f"Finished {task_id}")

with DAG(
    dag_id="manual_tasks_pool_test",
    start_date=datetime(2026, 4, 21),
    schedule=None,
    catchup=False,
    tags=["practice", "pools"]
) as dag:

    # 1. تاسك البداية
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS pool_test (
                id SERIAL PRIMARY KEY,
                task_name VARCHAR(50),
                execution_time TIMESTAMP
            );
        """
    )

    # 2. الـ 10 تاسكات بالشكل اللي طلبته بالظبط
    task_1 = PythonOperator(
        task_id="task_1",
        python_callable=my_python_function,
        op_args=["task_1"],
        pool='spark_pool',
        priority_weight=5
    )

    task_2 = PythonOperator(
        task_id="task_2",
        python_callable=my_python_function,
        op_args=["task_2"],
        pool='spark_pool',
        priority_weight=4
    )

    task_3 = PythonOperator(
        task_id="task_3",
        python_callable=my_python_function,
        op_args=["task_3"],
        pool='spark_pool',
        priority_weight=6
    )

    task_4 = PythonOperator(
        task_id="task_4",
        python_callable=my_python_function,
        op_args=["task_4"],
        pool='spark_pool',
        priority_weight=8
    )

    task_5 = PythonOperator(
        task_id="task_5",
        python_callable=my_python_function,
        op_args=["task_5"],
        pool='spark_pool',
        priority_weight=10
    )

    task_6 = PythonOperator(
        task_id="task_6",
        python_callable=my_python_function,
        op_args=["task_6"],
        pool='spark_pool',
        priority_weight=7
    )

    task_7 = PythonOperator(
        task_id="task_7",
        python_callable=my_python_function,
        op_args=["task_7"],
        pool='spark_pool',
        priority_weight=9
    )

    task_8 = PythonOperator(
        task_id="task_8",
        python_callable=my_python_function,
        op_args=["task_8"],pool='spark_pool',
        priority_weight=1
    )

    task_9 = PythonOperator(
        task_id="task_9",
        python_callable=my_python_function,
        op_args=["task_9"],pool='spark_pool',
        priority_weight=3
    )

    task_10 = PythonOperator(
        task_id="task_10",
        python_callable=my_python_function,
        op_args=["task_10"],
        pool='spark_pool',
        priority_weight=2
    )

    # 3. تاسك النهاية
    select_values = SQLExecuteQueryOperator(
        task_id="select_values",
        conn_id="postgres_default",
        sql="SELECT * FROM pool_test;"
    )

    
    create_table >> [task_1, task_2, task_3, task_4, task_5, task_6, task_7, task_8, task_9, task_10] >> select_values