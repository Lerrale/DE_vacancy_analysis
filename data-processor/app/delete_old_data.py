from datetime import datetime, timedelta
import logging
from db_connection import get_mongo_client

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    # Подключение к MongoDB
    client = get_mongo_client()
    db = client.vacancydb
    collection = db.vacancy

    # Определение даты 90 дней назад
    ninety_days_ago = datetime.now() - timedelta(days=90)

    # Преобразование в формат, совместимый с MongoDB
    ninety_days_ago_str = ninety_days_ago.strftime("%Y-%m-%dT%H:%M:%S%z")

    # Формирование запроса для поиска старых записей
    old_records = collection.find({"published_at": {"$lt": ninety_days_ago_str}})

    # Подсчет и удаление старых записей
    old_records_count = 0
    old_record_ids = []
    for record in old_records:
        old_record_ids.append(record['_id'])
        collection.delete_one({"_id": record['_id']})
        old_records_count += 1

    # Логирование результатов
    logging.info(f"Удалено записей: {old_records_count}")
    logging.info(f"ID удаленных записей: {old_record_ids}")

except Exception as e:
    logging.error(f"Произошла ошибка: {e}")
finally:
    if 'client' in locals():
        client.close()
        logging.info("MongoDB соединение закрыто")
