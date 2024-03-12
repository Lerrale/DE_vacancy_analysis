FROM python:3.8

WORKDIR /usr/app

# Копирование файла зависимостей в рабочий каталог
COPY requirements.txt ./

# Установка зависимостей
RUN pip install --no-cache-dir -r requirements.txt

# Копирование остальных файлов проекта в контейнер
COPY app/ .
RUN chmod +x /usr/app/*