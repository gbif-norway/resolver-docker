#!/bin/sh
python manage.py makemigrations
python manage.py migrate

# Set environment variables so they are accessible to cron
env >> /etc/environment

# Cron
chmod a+x manage.py
printf '%s\n\n' '0 0 * * MON python manage.py populate_resolver >> /var/log/cron_populate_resolver.log 2>&1' > /etc/cron.d/cron-jobs
crontab /etc/cron.d/cron-jobs
cron

# Start the server
# exec python manage.py runserver 0.0.0.0:8080
exec gunicorn resolver.wsgi:application --bind 0.0.0.0:8080 --worker-tmp-dir /dev/shm --workers=2 --threads=4 --worker-class=gthread
