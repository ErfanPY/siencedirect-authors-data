FROM mysql:5.7

EXPOSE 3306

ENV MYSQL_ROOT_PASSWORD="root"

ADD . /docker-entrypoint-initdb.d/
VOLUME /docker-entrypoint-initdb.d/

