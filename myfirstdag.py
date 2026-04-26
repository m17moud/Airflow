from airflow import DAG 
from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
with DAG (
    "firstdag", 
    start_date=datetime(2026, 4, 18),
    schedule="*/5 * * * *",
    catchup=False, 
    template_searchpath="/usr/local/airflow/dags",
    tags=["sales", "daily"] 
) as dag:

    Create_Table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres_default", 
        sql='sql/create_table.sql'


    )
    insert_values = SQLExecuteQueryOperator(
        task_id="insert_values",
        conn_id="postgres_default",
        sql='sql/insert.sql'
    )

    select_values = SQLExecuteQueryOperator(
        task_id="select_values",
        conn_id="postgres_default",
        sql="SELECT * FROM pet WHERE birth_date BETWEEN %(start_date)s AND %(end_date)s;",
        # لازم parameters تكون كلمة مفتاحية (Keyword Argument) جوه القوس
        parameters={
            "start_date": "{{ var.value.start_date }}", 
            "end_date": "{{ var.value.end_date }}"
        },show_return_value_in_logs=True
    ) # تأكد إن القوس مقفول هنا بعد الـ parameters



    Create_Table >> insert_values >> select_values 
