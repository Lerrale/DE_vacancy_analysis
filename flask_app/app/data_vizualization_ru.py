import warnings
import re
import ast
import os
from pymongo import MongoClient
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd


warnings.filterwarnings('ignore')

SAVE_PATH = "static/"

def export_data_to_csv(db_name, collection_name, csv_file_name):
    """
    Функция экспортирует данные из указанной коллекции MongoDB в формат CSV.

    Аргументы:
    db_name (str): Имя базы данных MongoDB.
    collection_name (str): Имя коллекции MongoDB.
    csv_file_name (str): Имя файла CSV, в который будут экспортированы данные.
    """
    MONGO_USER = os.getenv('MONGO_INITDB_ROOT_USERNAME', 'root')
    MONGO_PASSWORD = os.getenv('MONGO_INITDB_ROOT_PASSWORD', 'example')
    MONGO_HOST = os.getenv('MONGO_HOST', 'mongo')
    MONGO_PORT = os.getenv('MONGO_PORT', '27017')

    client = MongoClient(f'mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/')
    db = client[db_name]
    collection = db[collection_name]

    data = pd.DataFrame(list(collection.find()))
    data.to_csv(csv_file_name, index=False)

    client.close()

export_data_to_csv('vacancydb', 'vacancy', 'mongo_data.csv')

df = pd.read_csv("mongo_data.csv")

# Удаляем столбец с индексами
df.drop('_id', axis=1, inplace=True)


# РАСПРЕДЕЛЕНИЕ СРЕДНИХ ЗАРПЛАТ ДЛЯ ВАКАНСИЙ НА ДОЛЖНОСТЬ DATA ENGINEER


# Добавлена колонка salary - среднее между максимумом и минимумом в зарплдатной вилке
df['salary'] = (df.salary_from +df.salary_to)/2

# Строим гистограмму
mean_salary_common, axes = plt.subplots(nrows=1, ncols=1, figsize=(25, 10))
histplot2 = sns.histplot(data=df,
                         x="salary",
                         multiple="dodge",
                         shrink=1,
                         bins=40,
                         color='green')
histplot2.set_xlabel('Зарплата, $', fontsize=20)
histplot2.set_ylabel('Количество', fontsize=20)

# Сохранение графика
mean_salary_common.savefig(f'{SAVE_PATH}mean_salary_common_ru.png')

# Закрытие графика
plt.close(mean_salary_common)


# РАСПРЕДЕЛЕНИЕ ОБЪЯВЛЕНИЙ ПО СТРАНАМ


# Группируем данные для построения pie plot
df_pie_country = df.groupby('country').aggregate({'country': 'count'}) \
                 .rename(columns = {'country': 'country_count'}) \
                 .sort_values(by='country_count', ascending=False)

top_countries = df_pie_country.head(5).index

# Заменяем индексы на "Остальные" для стран,
# не входящих в топ-5
df_pie_country.index = df_pie_country.index.where(
    df_pie_country.index.isin(top_countries), 'Остальные'
    )
df_pie_country = df_pie_country.groupby(df_pie_country.index).sum()

# Строим диаграмму
pie_plt_country, axes = plt.subplots(nrows=1, ncols=1, figsize=(20, 7))
pie_plot = plt.pie(df_pie_country['country_count'],
            labels=df_pie_country.index,
            colors=sns.color_palette('pastel')[:len(df_pie_country)],
            autopct='%.0f%%'
            )

# Сохранение диаграммы
pie_plt_country.savefig(f'{SAVE_PATH}pie_plt_country_ru.png')

# Закрытие диаграммы
plt.close(pie_plt_country)


# СРЕДНЯЯ ЗАРПЛАТА В ЗАВИСИМОСТИ ОТ ОПЫТА РАБОТЫ


# Построение графиков
mean_salary_experiense, axes = plt.subplots(nrows=1, ncols=2, figsize=(20, 7))

custom_order = ['Нет опыта', 'От 1 года до 3 лет', 'От 3 до 6 лет', 'Более 6 лет']

barplot1 = sns.barplot(
           data=df,
           x='experience',
           y='salary',
           ci=None,
           ax=axes[0],
           order=custom_order
           )
barplot1.grid(True, linewidth=0.5, linestyle=':', color='gray', alpha=0.5)
barplot1.tick_params(axis='x', rotation=0)
barplot1.set_title('По всем странам',
                   fontsize=15, pad=22, fontweight='bold')
barplot1.set_xlabel('Требуемый опыт', fontsize=15, labelpad=15)
barplot1.set_ylabel('Средняя зарплата, $', fontsize=15, labelpad=15)


barplot2 = sns.barplot(
           data=df[df.country == 'Россия'],
           x='experience',
           y='salary',
           ci=None,
           ax=axes[1],
           order=custom_order
           )
barplot2.grid(True, linewidth=0.5, linestyle=':', color='gray', alpha=0.5)
barplot2.tick_params(axis='x', rotation=0)
barplot2.set_title('В России',
                   fontsize=15, pad=22, fontweight='bold')
barplot2.set_xlabel('Требуемый опыт', fontsize=15, labelpad=15)
barplot2.set_ylabel('Средняя зарплата, $', fontsize=15, labelpad=15)

# Сохранение графиков
mean_salary_experiense.savefig(f'{SAVE_PATH}mean_salary_experiense_ru.png')

# Закрытие графиков
plt.close(mean_salary_experiense)


# МАКСИМАЛЬНАЯ ЗАРПЛАТА В ЗАВИСИМОСТИ ОТ ОПЫТА РАБОТЫ


# Построение графиков
max_salary_experiense, axes = plt.subplots(nrows=1, ncols=2, figsize=(20, 7))

barplot1 = sns.barplot(
           data=df,
           x='experience',
           y='salary',
           ci=None,
           ax=axes[0],
           estimator='max',
           order=custom_order
           )
barplot1.grid(True, linewidth=0.5, linestyle=':', color='gray', alpha=0.5)
barplot1.tick_params(axis='x', rotation=0)
barplot1.set_title('По всем странам',
                   fontsize=15, pad=22, fontweight='bold')
barplot1.set_xlabel('Требуемый опыт', fontsize=15, labelpad=15)
barplot1.set_ylabel('Средняя зарплата, $', fontsize=15, labelpad=10)


barplot2 = sns.barplot(
           data=df[df.country == 'Россия'],
           x='experience',
           y='salary',
           ci=None,
           ax=axes[1],
           estimator='max',
           order=custom_order
           )
barplot2.grid(True, linewidth=0.5, linestyle=':', color='gray', alpha=0.5)
barplot2.tick_params(axis='x', rotation=0)
barplot2.set_title('В России',
                   fontsize=15, pad=22, fontweight='bold')
barplot2.set_xlabel('Требуемый опыт', fontsize=15, labelpad=15)
barplot2.set_ylabel('Средняя зарплата, $', fontsize=15, labelpad=10)

# Сохранение графиков
max_salary_experiense.savefig(f'{SAVE_PATH}max_salary_experiense_ru.png')

# Закрытие графиков
plt.close(max_salary_experiense)


# КЛЮЧЕВЫЕ НАВЫКИ


# Удаление строк, где нет значений в 'key_skills'.
# Для дальнейшего анализа они не понадобятся
df = df.dropna(subset=['key_skills'])

def convert_to_list(x):
    """
    Функция преобразует строковое представление списка в 
    фактический список Python, используя функцию 
    ast.literal_eval.

    Параметры:
    x (str): Строковое представление списка.

    Возвращает:
    list: Список, полученный из строкового представления.
    """
    return ast.literal_eval(x)

# Преобразование строк в списки
df['key_skills'] = df['key_skills'].apply(convert_to_list)

def clean_and_process_skills(skills):
    """
    Функция очищает и обрабатывает список навыков.

    Она очищает каждый навык от лишних символов, лишних 
    пробелов, заменяет тире на пробелы, удаляет скобки, 
    приводит к нижнему регистру и удаляет слово 'apache'.

    Параметры:
    skills (list): Список навыков, подлежащих обработке.

    Возвращает:
    list: Список очищенных и обработанных навыков.
    """
    cleaned_skills = []
    for skill in skills:
        skill = re.sub(r'\s+', ' ', skill)   # Удаление лишних пробелов
        skill = skill.replace('-', ' ')      # Замена тире на пробелы
        skill = re.sub(r'[();]', '', skill)  # Удаление скобок
        skill = skill.lower()                # Приведение к нижнему регистру
        skill = skill.replace('apache ', '') # Удаление 'apache'
        cleaned_skills.append(skill)
    return cleaned_skills

# Очистка ключевых навыков
df['key_skills'] = df['key_skills'].apply(clean_and_process_skills)

# Кодирование ключевых навыков. Из всех навыков, встречающихся
# в датасете создаем таблицу, в котором каждое наименование
# навыка равно одному столбцу. Наличие или отсутствие требования
# отмечается 1 и 0 соответвтенно
skills_encoded = pd.get_dummies(df['key_skills'].apply(pd.Series).stack()).groupby(level=0).sum()

# Объединение закодированных навыков с исходным датафреймом.
df = pd.concat([df, skills_encoded], axis=1)

# оставляем только развернутую таблицу навыков и опыт
df_skills = df.drop([
                     'vacancy_name',
                     'employer',
                     'city',
                     'salary_from',
                     'salary_to',
                     'published_at',
                     'url',
                     'key_skills',
                     'schedule',
                    'description',
                    'country',
                    'language',
                    'language_level',
                    'salary'
                     ], axis=1)

# Группируем по опыту
df_skills_grouped = df_skills.groupby('experience').sum()

df_skills_grouped = df_skills_grouped.astype('int64')

# Меняем местами строки и столбцы
df_skills_grouped = df_skills_grouped.transpose()

df_skills_grouped.columns = df_skills_grouped.columns.str.lower()

def bar_top_skills(experience):
    """
    Функция создает горизонтальный барплот который отображает самые 
    востребованные навыки по убыванию.

    Параметры:
        experience: Уровень опыта, для которого показываются навыки.
        
    Возвращает:
        барплот
    """
    # Маппинг названий для сохранения картинок
    save_dict = {'нет опыта': 'junior',
                'от 1 года до 3 лет': 'middle',
                'от 3 до 6 лет': 'senior',
                'более 6 лет': 'experienced_senior'}

    # Функция для формирования корректных названий к графикам
    def find_name_exp(experience):
        if experience == 'нет опыта':
            return 'без опыта'
        return 'с опытом ' + experience

    # Выводим только топ 30 скилов
    top = df_skills_grouped[experience].sort_values(ascending=False).head(30)
    fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(20, 12))

    barplot_de_skills = sns.barplot(
        x=top.values,
        y=top.index,
        orient='h',  # Ориентация горизонтальная
        ci=None
    )

    barplot_de_skills.tick_params(axis='y', rotation=0, labelsize=15)
    barplot_de_skills.tick_params(axis='x', rotation=0, labelsize=15)

    barplot_de_skills.set_title(
        f'Для специалистов {find_name_exp(experience)}',
        fontsize=20,
        pad=22,
        fontweight='bold')
    barplot_de_skills.set_xlabel('Встречаемость в объявлениях', fontsize=15, labelpad=10)
    barplot_de_skills.grid(True, linewidth=0.5, linestyle=':', color='gray', alpha=0.5)

    fig.savefig(f'{SAVE_PATH}top_skills_{save_dict[experience]}.png')
    plt.close(fig)


bar_top_skills('нет опыта')
bar_top_skills('от 1 года до 3 лет')
bar_top_skills('от 3 до 6 лет')
bar_top_skills('более 6 лет')
