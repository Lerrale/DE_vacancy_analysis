#!/bin/bash

echo "Начало инициализации базы данных Airflow..."
airflow db init
echo "Инициализация базы данных Airflow завершена."

echo "Обновление базы данных Airflow..."
airflow db upgrade
echo "Обновление базы данных Airflow завершено."

airflow users create \
  --username student44 \
  --firstname Admin \
  --lastname Admin \
  --role Admin \
  --email admin@example.com \
  --password jksdhgf65Jjh

echo "Запуск Triggerer..."
airflow triggerer &  

echo "Запуск Airflow webserver в фоновом режиме..."
airflow webserver -p 8080 & 

echo "Запуск Airflow Scheduler..."
exec airflow scheduler
