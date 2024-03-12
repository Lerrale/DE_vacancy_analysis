#!/usr/bin/env python
# coding: utf-8

import logging
import json
import time
import warnings
import requests
from tqdm import tqdm
import pandas as pd
import numpy as np
from db_connection import load_data_to_mongo, create_db_and_collection

warnings.filterwarnings('ignore')


# СБОР ДАННЫХ

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

res = []

NUM_PAGES = 20 # количество страниц для сбора информации
PAGINATION = 100 # количество вакансий на страницу <=100
LAST_N_DAYS = 3 # количество последних дней в рамках которых мы ищем вакансии

def get_vacancy(vacancies, pages=NUM_PAGES):
    """
    Функция возвращает список json-объектов, содержащих информацию 
    о вакансиях, найденных по ключевым словам на сайте hh.ru.

    Параметры:
        vacancies (list): список вакансий для поиска.
        pages : количество страниц для поиска.

    Возвращает:
        список json-объектов.
    """
    for vacancy in tqdm(vacancies, desc="Обработка вакансий"): # Прогресс бар
        logging.info(f'Начало получения информации по вакансии: {vacancy}')
        time.sleep(5)
        
        for page in tqdm(range(pages), desc=f'Загрузка страниц для {vacancy}'):
            logging.info(f'Обработка страницы {page + 1} для {vacancy}')
            request_params  = {
                     'text': f'{vacancy}',
                     'page': page,
                     'per_page': PAGINATION,
                     'only_with_salary': 'false',
                     'period': LAST_N_DAYS
                     }
            req = requests.get('https://api.hh.ru/vacancies',
                               params=request_params,
                               timeout=60
                              ).json()
            # print(pd.json_normalize(req))
            if 'items' in req.keys():
                res.extend(req['items'])
                # print(pd.json_normalize(req['items']))
            time.sleep(1)
        logging.info(f'Завершение обработки вакансии: {vacancy}')    

vacancy_list = ['NAME:"data engineer"',
             'NAME:"data-engineer"'
             'NAME:"дата инженер"',
             'NAME:"дата-инженер"'
            ]

get_vacancy(vacancy_list)


data = pd.json_normalize(res)

# Сохранение сырых данных
data.to_csv('initial_data.csv', index=False)

df_test = pd.read_csv("initial_data.csv")


# ОБОГОАЩЕНИЕ НОВЫМИ ДАННЫМИ


df = pd.read_csv("initial_data.csv")

# Колонки для дальнейшей работы
columns = ['name',
           'employer.name',
           'area.name',
           'salary.from',
           'salary.to',
           'salary.currency',
           'experience.name',
           'published_at',
           'url',
           'id'
           ]

df = df[columns]

def reach_vacancy(url):
    """
    Функция извлекает информацию о вакансии из предоставленного URL
    такую как ключевые данные о вакансии, такие как навыки, языки,
    график работы и описание. Данные извлекаются из JSON-ответа.

    Параметры:
        url (str): URL адрес вакансии, откуда будут извлекаться данные.
 
    Возвращает:
        list : список, из 4х элементов, который содержит (по порядку):
        - key_skills
        - languages
        - schedule
        - description
        
    Примечание:
    Функция использует задержку в 0.5 секунды перед отправкой запроса,
    чтобы избежать капчи.
    """
    logging.info(f'Начало обогащения данных для URL: {url}')
    feature_list = []
    try:
        time.sleep(0.5)  # Задержка для избежания капчи
        req = requests.get(url, timeout=60).json()
        if 'key_skills' in req:
            feature_list.append(req['key_skills'])
        else:
            feature_list.append(None)

        if 'languages' in req:
            feature_list.append(req['languages'])
        else:
            feature_list.append(None)

        if 'schedule' in req:
            feature_list.append(req['schedule'])
        else:
            feature_list.append(None)

        if 'description' in req:
            feature_list.append(req['description'])
        else:
            feature_list.append(None)

        logging.info(f'Успешно извлечена информация для URL: {url}')

    except Exception as e:
        logging.error(f'Ошибка при извлечении данных для URL: {url} | Ошибка: {e}')
        feature_list = [None, None, None, None]  # Возвращаем список с None, если произошла ошибка

    return feature_list

df['reached_skills'] = df['url'].apply(reach_vacancy)

df['key_skills'] = df['reached_skills'].apply(lambda x: x[0])
df['languages'] = df['reached_skills'].apply(lambda x: x[1])
df['schedule'] = df['reached_skills'].apply(lambda x: x[2])
df['description'] = df['reached_skills'].apply(lambda x: x[3])

# Удаление столбца reached_skills
df = df.drop(['reached_skills'], axis=1)

df.to_csv('reached_data.csv', index=False)


# ОБРАБОТКА ДАННЫХ


df = pd.read_csv("reached_data.csv")

df.rename(columns = {'salary.from':'salary_from',
                     'salary.to':'salary_to',
                     'area.name':'city',
                     'employer.name':'employer',
                     'experience.name':'experience',
                     'name':'vacancy_name'
                    }, inplace = True )


# Колонка id


# Удаление дубликатов по id
df = df.drop_duplicates(subset='id')

# Удаление колонки id
df = df.drop(['id'], axis=1)


# Колонка salary


# Медианная разница между верхней и нижней границей зарплатной вилки,
# посчитанная отдельно.
MEDIAN_DIF = 0.43

# Заполним значения salary_to когда есть salary_from но нет
# salary_to, используя полученное медианное значение разницы
# в зарплатной вилке.
condition_sal_from = df.salary_from.notnull() & df.salary_to.isnull()
df.loc[condition_sal_from, 'salary_to'] = (
                                            df.salary_from[condition_sal_from]
                                          ) * (MEDIAN_DIF) + (df.salary_from[condition_sal_from])

# Заполним значения salary_from когда нет salary_from но есть
# salary_to, используя полученное медианное значение разницы
# в зарплатной вилке.
condition_sal_to = df.salary_from.isnull() & df.salary_to.notnull()
df.loc[condition_sal_to, 'salary_from'] = (df.salary_to[condition_sal_to])/(1+MEDIAN_DIF)

# Смена кодировки валют на международную
df.loc[(df['salary.currency'] == 'RUR'), 'salary.currency'] = 'RUB'
df.loc[(df['salary.currency'] == 'BYR'), 'salary.currency'] = 'BYN'

currency_list = list(df['salary.currency'].unique())

# Удаление nan из списка currency_list
currency_list = [x for x in currency_list if not (isinstance(x, float) and np.isnan(x))]

# Удалим USD так как все валюты будем приводить к нему
if 'USD' in currency_list:
    currency_list.remove('USD')

# Достанем актуальные крусы валют
def get_currency_rates(currency_codes):
    """
    Функция извлекает актуальные курсы валют по отношению к доллару с помощью API.
    
    Аргументы:
    currency_codes (list): Список валютных кодов, для которых необходимо получить курсы.
    
    Возвращает:
    dict: Словарь, где ключами являются валютные коды из currency_codes, а 
          значениями - соответствующие курсы. В случае возникновения 
          ошибки при выполнении запроса к API возвращает строку с сообщением об ошибке.
    """
    # API URL для получения текущих курсов валют
    api_url = "https://api.exchangerate-api.com/v4/latest/USD"

    try:
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()  # Вызываем исключение, если запрос не удался
        response_data = response.json()

        # Извлекаем курсы валют
        rates = response_data.get('rates', {})

        # Получаем курсы для указанных валют
        cur_rates = {currency: rates.get(currency, 'Not Found') for currency in currency_codes}

        return cur_rates

    except requests.RequestException as e:
        return str(e)

currency_rates = get_currency_rates(currency_list)

# Приведем все валюты к доллару
for i in currency_list:
    mask_cur = (df['salary.currency'] == i)
    df.loc[mask_cur, 'salary_from'] /= currency_rates[i]
    df.loc[mask_cur, 'salary_to'] /= currency_rates[i]

# Столбец salary.currency больше не нужен, удалим его.
df = df.drop(['salary.currency'], axis=1)

# Округлим полученные знаяения
mask_astype_from = df.salary_from.notnull()
mask_astype_to = df.salary_to.notnull()
df.loc[mask_astype_from, 'salary_from'] = df.loc[mask_astype_from, 'salary_from'].astype('int64')
df.loc[mask_astype_to, 'salary_to'] = df.loc[mask_astype_to, 'salary_to'].astype('int64')


# Столбец area.name


# Дополним данные названием стран, которые можно определить по названию города

# Список имеющихся городов
city_list = list(df['city'].unique())

def get_city_country_catalog():
    """
    Получает каталог городов и стран из API HeadHunter.

    Возвращает:
        dict: Каталог городов и стран в формате JSON.
    """
    response = requests.get('https://api.hh.ru/areas', timeout=60)
    response.raise_for_status()
    city_country_cat = response.json()
    return city_country_cat

city_country_catalog = get_city_country_catalog()

def find_country_by_city(city_name):
    """
    Находит название страны по заданному названию города.

    Аргументы:
        city_name (str): Название города, для которого нужно найти соответствующую страну.

    Возвращает:
        str: Название страны, если найдено, в противном случае "Страна не найдена".
    """
    def search_area(area, city_name):
        """
        Рекурсивная функция для поиска города в странах и районах.
        Сначала она смотрит, является ли название текущего района искомым городом.
        Если нет, то функция начинает итерацию по всем подрайонам.

        Аргументы:
            area (dict): Словарь, представляющий страны и районы с городами.
            city_name (str): Название города для поиска.

        Возвращает:
            bool: True, если город найден в стране/районе/подрайоне,
                  False в противном случае.
        """
        if area['name'] == city_name:
            return True
        for sub_area in area.get('areas', []):
            if search_area(sub_area, city_name): # Если функция вернула результат
                return True
        return False

    for country in city_country_catalog:
        if country['name'] == "Другие регионы":  # Если название страны "Другие регионы",
                                                 # значит настоящее название страны вложено
                                                 # внутрь, и лежит там где обычно города
            for area in country['areas']:
                if search_area(area, city_name): # Если функция search_area вернула True
                    return city_name             # city_name и будет названием страны
        elif search_area(country, city_name):
            return country['name']

    return "Страна не найдена"

df['country'] = df['city'].apply(find_country_by_city)

df.to_csv('reached_data2.csv', index=False)


# #### languages


df = pd.read_csv("reached_data2.csv")

def extract_id(languages_str):
    """
    Извлекает идентификатор языка из строки, представляющей список языков в формате 
    похожем на JSON.

    Аргументы:
        languages_str (str): Строка, представляющая список языков в формате
        похожем на JSON.

    Возвращает:
        str или None: Идентификатор языка, если он найден, в противном случае None.
    """
    if not languages_str or languages_str == '[]':
        return None

    try:
        # Заменяем одинарные кавычки на двойные
        corrected_str = languages_str.replace("'", '"')

        # Преобразуем исправленную строку в список словарей
        languages_list = json.loads(corrected_str)

        if languages_list and isinstance(languages_list, list):
            return languages_list[0].get('id')
    except Exception:
        pass
    return None

def extract_level(languages_str):
    """
    Извлекает идентификатор уровня знания языка из строки, представляющей список языков в формате
    похожем на JSON.

    Аргументы:
        languages_str (str): Строка, представляющая список языков в формате 
        похожем на JSON.

    Возвращает:
        str или None: Идентификатор уровня знания языка, если он найден, в противном случае None.
    """
    if not languages_str or languages_str == '[]':
        return None

    try:
        # Заменяем одинарные кавычки на двойные
        corrected_str = languages_str.replace("'", '"')

        # Преобразуем исправленную строку в список словарей
        languages_list = json.loads(corrected_str)

        if languages_list and isinstance(languages_list, list):
            level = languages_list[0].get('level')
            return level['id']
    except Exception:
        pass
    return None

df['language'] = df['languages'].apply(extract_id)
df['language_level'] = df['languages'].apply(extract_level)

# Удаление столбца languages.
df = df.drop(['languages'], axis=1)


# Столбец schedule


def extract_schedule(schedule_str):
    """
    Извлекает идентификатор графика работы из строки, представляющей график в формате 
    похожем на JSON.

    Аргументы:
        schedule_str (str): Строка, представляющая график работы в формате 
        похожем на JSON.

    Возвращает:
        str или None: Идентификатор графика работы, если он найден, в противном случае None.
    """
    if not schedule_str or schedule_str == '[]':
        return None

    try:
        # Заменяем одинарные кавычки на двойные
        corrected_str = schedule_str.replace("'", '"')

        schedule_json = json.loads(corrected_str)

        schedule = schedule_json.get('id')
        return schedule
    except Exception:
        pass
    return None

df['schedule'] = df['schedule'].apply(extract_schedule)


# #### key_skills

def extract_key_skills(key_skills_str):
    """
    Извлекает ключевые навыки из строки, представляющей список ключевых навыков в формате
    похожем на JSON.

    Аргументы:
        key_skills_str (str): Строка, представляющая список ключевых навыков в формате
        похожем на JSON.

    Возвращает:
        list или None: Список ключевых навыков, если они найдены, в противном случае None.
    """
    if not key_skills_str or key_skills_str == '[]':
        return None

    try:
        # Заменяем одинарные кавычки на двойные
        corrected_str = key_skills_str.replace("'", '"')

        skill_list_json = json.loads(corrected_str)

        if skill_list_json and isinstance(skill_list_json, list):
            skill_list = []
            for i in skill_list_json:
                skill_list.append(i['name'])
            return skill_list
    except Exception:
        pass
    return None


df.key_skills = df.key_skills.apply(extract_key_skills)


# Столбец published_at


# Не используется
def extract_date (full_date):
    """
    Извлекает дату из строки, содержащей полную дату и время.

    Аргументы:
        full_date (str): Строка, содержащая полную дату и время.

    Возвращает:
        str или None: Строка с датой в формате 'день.месяц.год', 
        если дата извлечена успешно, в противном случае None.
    """
    try:
        date_list = full_date.split('T')
        date = pd.to_datetime(date_list[0])
        formatted_date = date.strftime('%d.%m.%Y')
        return formatted_date

    except Exception:
        pass
    return None


df.to_csv('processed_data.csv', index=False)


#  ЗАГРУХКА ДАННЫХ В MONGO


df = pd.read_csv("processed_data.csv")

# Конвертация DataFrame в формат словаря для MongoDB
data_records = df.to_dict(orient='records')

# Создание новой базы и коллекции
create_db_and_collection('vacancydb', 'vacancy')

# Добавление данных в выбранную базу и коллекцию
load_data_to_mongo('vacancydb', 'vacancy', data_records)
