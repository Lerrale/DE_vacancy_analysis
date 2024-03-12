import logging
from db_connection import get_mongo_client

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    # Подключение к MongoDB
    client = get_mongo_client()
    db = client.vacancydb
    collection = db.vacancy

    # Агрегационный запрос для идентификации дубликатов по URL
    pipeline = [
        {
            "$group": {
                "_id": "$url",
                "allIds": {"$push": "$_id"},
                "publishedAt": {"$push": "$published_at"}
            }
        },
        {
            "$match": {
                "$expr": {
                    "$gt": [{"$size": "$allIds"}, 1]
                }
            }
        }
    ]

    duplicates = collection.aggregate(pipeline)

    # Удаление дубликатов
    for duplicate in duplicates:
        ids = duplicate['allIds']
        dates = duplicate['publishedAt']

        if len(ids) > 1:
            # Сортировка ID по дате публикации, сохранение самого нового
            sorted_ids = [id for _, id in sorted(zip(dates, ids), reverse=True)]

            # Удаление всех, кроме самого нового
            collection.delete_many({"_id": {"$in": sorted_ids[:-1]}})

            logging.info(f"Удалены дубликаты для URL {duplicate['_id']}, оставлен {sorted_ids[-1]}")

except Exception as e:
    logging.error(f"Произошла ошибка: {e}")

finally:
    client.close()
    logging.info("MongoDB соединение закрыто")
