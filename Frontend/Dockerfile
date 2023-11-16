FROM python:3.8-slim-buster

RUN apt update -y && apt install -y build-essential libpq-dev
RUN apt-get update && apt-get install -y gcc g++ libffi-dev

COPY webapp/requirements.txt .
RUN pip install -r requirements.txt

COPY webapp/ ./

EXPOSE 80

CMD [ "gunicorn", "-b 0.0.0.0:80", "application:application"]