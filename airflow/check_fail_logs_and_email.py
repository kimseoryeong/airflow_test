from airflow import DAG
from airflow.decorators import task
from airflow.operators.email import EmailOperator
from airflow.hooks.base import BaseHook
from datetime import timedelta
import pendulum
import psycopg2
import logging

with DAG(
    dag_id='check_fail_logs_and_email',
    schedule='0 8 1 * *',  # 매월 1일 오전 8시
    start_date=pendulum.datetime(2023, 3, 1, tz='Asia/Seoul'),
    catchup=False,
    tags=['alert', 'email'],
) as dag:

    @task(task_id="check_fail_db_logs")
    def check_fail_logs():
        conn = BaseHook.get_connection("azure_postgres")

        pg_conn = psycopg2.connect(
            host=conn.host,
            port=conn.port,
            database=conn.schema,
            user=conn.login,
            password=conn.password
        )

        cursor = pg_conn.cursor()
        cursor.execute("""
            SELECT job_name, message, run_time 
            FROM etl_job_logs 
            WHERE status = 'fail' AND run_time::date = CURRENT_DATE;
        """)
        fail_logs = cursor.fetchall()
        cursor.close()
        pg_conn.close()

        if not fail_logs:
            logging.info("✅ No fail logs found for today.")
            return "No failures today 🎉"

        # 메시지 포맷팅
        html_msg = "<h4>🚨 Fail Logs Detected</h4><ul>"
        for job_name, message, run_time in fail_logs:
            html_msg += f"<li><b>{job_name}</b>: {message} <br> at {run_time}</li>"
        html_msg += "</ul>"

        return html_msg  # ✅ XCom으로 메시지 전달

    # 이메일 전송 태스크
    # send_email_alert = EmailOperator(
    #     task_id='send_email_alert',
    #     to='srkim@zenithcloud.com',
    #     subject='{{ dag_run.logical_date.astimezone(pytz.timezone("Asia/Seoul")).strftime("%Y-%m-%d") }} ETL 실패 로그 알림',
    #     html_content="""
    #     {{ ti.xcom_pull(task_ids='check_fail_db_logs') }}
    #     """,
    # )
    send_email_alert = EmailOperator(
        task_id="send_email_alert",
        to="srkim@zenithcloud.com",
        subject="{{ ti.xcom_pull(task_ids='check_fail_db_logs')['subject'] }} ETL 실패 로그 알림",
        html_content="{{ ti.xcom_pull(task_ids='check_fail_db_logs')['html'] }}",
    )

    check_fail_logs() >> send_email_alert