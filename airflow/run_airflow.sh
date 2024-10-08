#!/bin/bash

echo "Начало инициализации базы данных Airflow..."
airflow db init
echo "Инициализация базы данных Airflow завершена."

echo "Обновление базы данных Airflow..."
airflow db upgrade
echo "Обновление базы данных Airflow завершено."

airflow users create \
  --username Admin \
  --firstname Admin \
  --lastname Admin \
  --role Admin \
  --email admin@example.com \
  --password Admin

echo "Запуск Triggerer..."
airflow triggerer &  

echo "Запуск Airflow webserver в фоновом режиме..."
airflow webserver -p 8080 & 

echo "Запуск DAG data_processing..."
airflow dags unpause data_processing

echo "Запуск Airflow Scheduler..."
exec airflow scheduler
