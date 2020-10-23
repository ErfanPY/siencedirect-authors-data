Sciencedirect scraper :

Search articles with given search parameters
Get authors data from articles
Authors data: Name, Email, Scopus_id


## REQUIREMENT

redis-server
mysql
python-celery-common
+ Python packages in requirement.txt

## RUN

# Virtual environment
virtualenv venv
source ./venv/bin/activate

# REDIS
docker run --name redis_serv -p 6379:6379 redis redis-server --save "" --port 6379

# MYSQL
mysql -uroot -p < ./db/scripts/sciencedirect_complete_clear.sql 

# CELERY
celery -A get_sd_ou.get_sd_ou.celery worker -Q main_search --loglevel=INFO -E -P eventlet -c 100
celery -A get_sd_ou.get_sd_ou.celery worker -Q scopus_search --loglevel=INFO -E

# PYTHON
python3 -m get_sd_ou.app
