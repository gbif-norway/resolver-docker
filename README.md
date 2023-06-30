[![run-tests](https://github.com/gbif-norway/resolver-docker/actions/workflows/run-tests.yml/badge.svg)](https://github.com/gbif-norway/resolver-docker/actions/workflows/run-tests.yml)

Create key value pairs ("FOO=BAR", 1 per line) for environment settings in either prod.env or dev.env for docker deployment. 
To run: `docker-compose -f docker-compose.dev.yml up` 
Once a dev docker container is up and running, enter it with `docker exec -it resolver-docker_web_1 /bin/bash` and run `python manage.py test to run tests.`

To build & push: `docker build . -t gbifnorway/resolver:latest` and `docker push gbifnorway/resolver:latest`
