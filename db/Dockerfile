FROM mysql:5.7

EXPOSE 3306

ENV MYSQL_ROOT_PASSWORD="root"

ADD . /var/lib/mysql/
RUN chmod 700 /var/lib/mysql/ 
VOLUME /var/lib/mysql/

