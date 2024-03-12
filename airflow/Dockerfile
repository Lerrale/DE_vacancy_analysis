# FROM apache/airflow:latest
FROM apache/airflow:slim-latest-python3.7

# Установка рабочей директории
WORKDIR /opt/airflow

# Копирование DAGs и логов
COPY dags/ ./dags
COPY logs/ ./logs

# Копирование скрипта для запуска Airflow
COPY run_airflow.sh ./run_airflow.sh

# Запуск скрипта при старте контейнера
ENTRYPOINT ["./run_airflow.sh"]

# docker build -t my_airflow .
# docker run -it --name my_airflow my_airflow
# docker exec -it 7bd7d6dbf3b9 bash


