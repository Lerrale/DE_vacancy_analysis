FROM python:3.11.1

WORKDIR /usr/src/app

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY app/ .

RUN chmod +x /usr/src/app/*

CMD ["/usr/src/app/run.sh"]