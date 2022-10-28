FROM python:3.9-slim
ENV PYTHONUNBUFFERED 1
WORKDIR /code
COPY . /code
ADD https://jdbc.postgresql.org/download/postgresql-42.2.6.jar /srv/postgresql-42.2.6.jar
RUN apt update && apt install -y default-jre
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
