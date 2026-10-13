FROM python:3.12-slim-bookworm
ENV PYTHONUNBUFFERED=1
WORKDIR /srv
COPY ./src /srv
COPY ./requirements.txt /srv/requirements.txt
RUN apt-get update && \
    apt-get install -y --no-install-recommends cron && \
    rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir -r requirements.txt
COPY ./entrypoint.sh /srv/entrypoint.sh
ENTRYPOINT ["/srv/entrypoint.sh"]
