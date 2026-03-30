from airflow import DAG
from airflow.operators.email import EmailOperator
from datetime import datetime

with DAG(
    dag_id="smtp_email_example",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    send_email = EmailOperator(
        task_id="send_test_email",
        to="sharontan@kenanga.com.my",
        subject="Airflow SMTP Test",
        html_content="""
        <h3>Email from Airflow</h3>
        <p>This email is sent using SMTP connection.</p>
        """
    )