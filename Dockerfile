FROM python:3.7-slim
ENV PYTHONUNBUFFERED 1
WORKDIR /srv
COPY ./src /srv
COPY ./requirements.txt /srv/requirements.txt
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
COPY ./entrypoint.sh /srv/entrypoint.sh
ENTRYPOINT ["/srv/entrypoint.sh"]
