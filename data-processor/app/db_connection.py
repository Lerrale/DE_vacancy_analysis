import os
from pymongo import MongoClient

# Определение переменных окружения для подключения к MongoDB
MONGO_USER = os.getenv('MONGO_INITDB_ROOT_USERNAME', 'root')
MONGO_PASSWORD = os.getenv('MONGO_INITDB_ROOT_PASSWORD', 'example')
MONGO_HOST = os.getenv('MONGO_HOST', 'mongo')
MONGO_PORT = os.getenv('MONGO_PORT', '27017')

def get_mongo_client():
    """
    Создает и возвращает клиент MongoDB.

    Использует переменные окружения для получения данных аутентификации и адреса сервера MongoDB.
 
    Returns:
        MongoClient: Объект клиента MongoDB для взаимодействия с базой данных.
    """
    client = MongoClient(f'mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/')
    return client

def create_db_and_collection(db_name, collection_name):
    """
    Создает базу данных и коллекцию в MongoDB, если они еще не существуют.

    Параметры:
        db_name (str): Имя создаваемой базы данных.
        collection_name (str): Имя создаваемой коллекции в базе данных.

    Описание:
        Подключается к MongoDB используя стандартные учетные данные.
        Проверяет, существует ли указанная база иликоллекция в базе данных,
        и если нет, создает ее.
    """
    client = get_mongo_client()
    db = client[db_name]
    if collection_name not in db.list_collection_names():
        db.create_collection(collection_name)
    client.close()

def load_data_to_mongo(db_name, collection_name, data):
    """
    Загружает данные в указанную коллекцию MongoDB.

    Параметры:
        db_name (str): Имя базы данных MongoDB.
        collection_name (str): Имя коллекции в базе данных.
        data (list): Список данных для загрузки в коллекцию.

    Описание:
        Создает клиент MongoDB, получает доступ к указанной базе данных
        и коллекции, и загружает в нее переданные данные. После 
        выполнения операции закрывает соединение с MongoDB.
    """
    client = get_mongo_client()
    db = client[db_name]
    collection = db[collection_name]
    collection.insert_many(data)
    client.close()
