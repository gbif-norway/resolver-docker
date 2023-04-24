#!/bin/sh
python manage.py makemigrations
python manage.py migrate

# Start the server
# exec python manage.py runserver 0.0.0.0:8080
exec gunicorn resolver.wsgi:application --bind 0.0.0.0:8080 --worker-tmp-dir /dev/shm --workers=2 --threads=4 --worker-class=gthread

