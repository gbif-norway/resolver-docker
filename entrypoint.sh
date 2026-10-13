#!/bin/sh
set -eu

python manage.py migrate --noinput

# Cron does not inherit the container environment. Keep its copy of the
# database credentials readable only by root.
python - <<'PY'
import os
import re
import shlex

path = '/run/resolver-cron.env'
with open(path, 'w') as cron_env:
    os.chmod(path, 0o600)
    for key, value in os.environ.items():
        if re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', key):
            cron_env.write('export {}={}\n'.format(key, shlex.quote(value)))
PY

printf '%s\n' '0 0 * * MON . /run/resolver-cron.env && /usr/local/bin/python /srv/manage.py populate_resolver >> /var/log/cron_populate_resolver.log 2>&1' > /etc/cron.d/cron-jobs
crontab /etc/cron.d/cron-jobs
cron

exec gunicorn resolver.wsgi:application --bind 0.0.0.0:8080 --worker-tmp-dir /dev/shm --workers=2 --threads=4 --worker-class=gthread
