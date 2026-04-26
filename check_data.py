from airflow import DAG 
from datetime import datetime, timedelta
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.standard.operators.python import PythonOperator 
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.task.trigger_rule import TriggerRule

# 1. تعريف وظيفة التنبيه عند تأخر الـ SLA
def sla_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    وظيفة احترافية ترسل تنبيه مخصص عند حدوث تأخير
    """
    print(f"SLA Missed for DAG: {dag.dag_id}")
    print(f"Tasks that missed SLA: {task_list}")
    
    # هنا ممكن نستخدم EmailOperator يدوي أو نكتفي بالـ Logs 
    # Airflow سيبعت إيميل تلقائي لو الإعدادات صحيحة

with DAG (
    "check_data", 
    start_date=datetime(2026, 4, 19),
    schedule="*/5 * * * *",
    catchup=False, 
    template_searchpath="/usr/local/airflow/dags",
    tags=["sales", "daily"],
    # 2. ربط الـ Callback بالـ DAG
    sla_miss_callback=sla_callback,
    default_args={
        'email': ['m.h.tr155@gmail.com'], # الإيميل اللي هيستلم تنبيه الـ SLA
        'email_on_failure': False,
        'email_on_retry': False,
    }
) as dag:

    # 3. تحديد الـ SLA للسنسور
    # لو السنسور فضل يعمل Poke ومقاش الصف في خلال 30 ثانية، هيعتبر SLA Miss
    check_records = SqlSensor(
        task_id="check_records",
        conn_id="postgres_default",
        sql="SELECT * FROM pet WHERE owner = 'Mahmoud_Hassan';",
        poke_interval=10,
        timeout=60, # الوقت الكلي قبل الفشل
        mode="reschedule",
        soft_fail=False,
        sla=timedelta(seconds=30) # التنبيه هيشتغل لو مخلصتش في 30 ثانية
    )

    processing_data = PythonOperator(
        task_id="processing_data",
        python_callable=lambda: print("Row added and processed!")
    )

    alerts = EmailOperator(
        task_id="task_alert",
        to="m.h.tr155@gmail.com",
        subject="Pipeline Alert: Data Missing",
        html_content="The sensor failed to find the record.",
        conn_id="smtp_default",
        from_email="mahmoudhassanbrhm@gmail.com",
        trigger_rule=TriggerRule.ONE_FAILED 
    )

    check_records >> [processing_data, alerts]