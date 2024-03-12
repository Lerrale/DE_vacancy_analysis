from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

# Определение аргументов по умолчанию
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 2, 14, 10, 0, tz='UTC'),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(hours=1),
}

# Определение DAG
dag = DAG('data_processing',
          default_args=default_args,
          description='Daily data processing',
          schedule_interval='0 9 * * *', # Запуск в 9:00 UTC каждый день, что соответствует 12:00 MSK
          catchup=False)

# Первая задача: запуск обработчика данных
t1 = BashOperator(
    task_id='run_data_processor',
    bash_command='/opt/airflow/dags/run_data_processor.sh ',
    dag=dag)

# Вторая задача: удаление дубликатов, выполняется после t1
t2 = BashOperator(
    task_id='run_delete_duplicates',
    bash_command='/opt/airflow/dags/run_delete_dublicates.sh ',
    dag=dag)

# Третья задача: удаление страрых записей (старше 90 дней), выполняется после t2
t3 = BashOperator(
    task_id='delete_old_data',
    bash_command='/opt/airflow/dags/run_delete_old_data.sh ',
    dag=dag)

# Четвертая задача: визуализация обновленных данных
t4 = BashOperator(
    task_id='data_vizualization',
    bash_command='/opt/airflow/dags/run_vizualization.sh ',
    dag=dag)

# Установка зависимостей между задачами
t1 >> t2 >> t3 >> t4
