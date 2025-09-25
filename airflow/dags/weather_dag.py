from datetime import datetime, timedelta
from pendulum import timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
import sys
import os

# 将脚本目录添加到Python路径
sys.path.append('/opt/airflow/scripts')

# 默认参数
default_args = {
    'owner': 'weather_team',
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
    'sla': timedelta(minutes=45)
}

# DAG定义
with DAG(
    'weather_data_pipeline',
    default_args=default_args,
    description='每小时采集和风天气数据并存储到数据库',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2025, 1, 1, tzinfo=timezone("Asia/Shanghai")),
    catchup=False,
    max_active_runs=1,
    tags=['weather', 'etl', 'qweather'],
    doc_md="""## 天气数据管道
    这个DAG每小时执行一次，从和风天气API获取多个城市的实时天气数据，
    经过清洗转换后存储到PostgreSQL数据库。
    """
) as dag:

    # 任务1: 开始标记
    start = DummyOperator(
        task_id='start_pipeline',
        dag=dag
    )

    # 任务2: 执行ETL
    def run_etl():
        """执行ETL任务的Python可调用函数"""
        from weather_etl import main
        main()

    etl_task = PythonOperator(
        task_id='fetch_and_process_weather_data',
        python_callable=run_etl,
        execution_timeout=timedelta(minutes=15),
        retries=2,
        dag=dag
    )

    # 任务3: 数据质量检查
    def check_data_quality():
        """检查数据质量"""
        from sqlalchemy import create_engine, text
        import pandas as pd
        
        # 获取数据库连接
        engine = create_engine(
            f'postgresql://{os.environ["POSTGRES_USER"]}:{os.environ["POSTGRES_PASSWORD"]}@postgres:5432/{os.environ["POSTGRES_DB"]}'
        )
        
        # 检查最近一小时是否有数据
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) as count, city_name 
                FROM weather_now 
                WHERE insert_ts >= NOW() - INTERVAL '1 hour'
                GROUP BY city_name
            """))
            
            counts = result.fetchall()
            
            # 检查每个城市都有数据
            expected_cities = os.environ.get('QWEATHER_LOCATION_NAMES', '').split(',')
            actual_cities = [row[1] for row in counts]
            
            missing_cities = set(expected_cities) - set(actual_cities)
            
            if missing_cities:
                raise ValueError(f"Missing data for cities: {missing_cities}")
            
            # 检查数据量
            for count, city in counts:
                if count == 0:
                    raise ValueError(f"No data found for {city} in the last hour")
                
            print(f"Data quality check passed. Records found: {dict(counts)}")

    quality_check = PythonOperator(
        task_id='data_quality_check',
        python_callable=check_data_quality,
        retries=1,
        dag=dag
    )

    # 任务4: 成功通知
    def send_success_email(**context):
        import smtplib
        from email.mime.text import MIMEText
        
        msg = MIMEText(context['templates_dict']['html_content'], 'html')
        msg['Subject'] = context['templates_dict']['subject']
        msg['From'] = 'chunpingchan@163.com'
        msg['To'] = 'chunpingchan@163.com'
        
        try:
            with smtplib.SMTP_SSL('smtp.163.com', 465) as server:
                server.login('chunpingchan@163.com', 'GDvFCNTfywYMhafB')
                server.send_message(msg)
        except Exception as e:
            raise Exception(f"Email sending failed: {str(e)}")

    success_notification = PythonOperator(
        task_id='send_success_email',
        python_callable=send_success_email,
        templates_dict={
            'html_content': """<h3>Weather Data ETL Pipeline Completed Successfully</h3>...""",
            'subject': 'Weather ETL Pipeline Succeeded - {{ ds }}'
        },
        provide_context=True,
        dag=dag
    )

    # 任务5: 结束标记
    end = DummyOperator(
        task_id='end_pipeline',
        dag=dag
    )

    # 定义任务依赖关系
    start >> etl_task >> quality_check >> success_notification >> end

    # 文档
    dag.doc_md = __doc__

# 只有在直接运行时才测试
if __name__ == "__main__":
    dag.test()