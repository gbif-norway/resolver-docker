Create key value pairs ("FOO=BAR", 1 per line) for environment settings in either prod.env or dev.env for docker deployment. 
To run: `docker-compose -f docker-compose.dev.yml up` 
Once a dev docker container is up and running, enter it with `docker exec -it resolver-docker_web_1 /bin/bash` and run `python manage.py test to run tests.`
