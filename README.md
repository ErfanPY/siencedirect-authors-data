## Sciencedirect scraper :

[Sciencedirect](https://www.sciencedirect.com/) Authors scraper.

It has a flask user interface and celery as task queue,
You have to start Celery and then run the flask server .

You can choose the search parameters (same as sciencedirect article search parameters)  for  the form in the index page.
The it will start a celery task and search for authors , it returns a search status every 2 second.

Also you can goto database result search page and search for scraped article and authors data.


## REQUIREMENT

redis-server  
mysql  
python-celery-common  
+ Python packages in requirement.txt

## RUN

# Virtual environment
```
virtualenv venv  
source ./venv/bin/activate  
pip install -r ./requirement.txt  
```
# REDIS
```
docker run --name redis_serv -p 6379:6379 redis redis-server --save "" --port 6379
```
# MYSQL
```
mysql -uroot -p < ./db/scripts/sciencedirect_complete_clear.sql 
```
# CELERY
```
celery -A get_sd_ou.get_sd_ou.celery worker -Q main_search --loglevel=DEBUG -E -P eventlet -c 100  > log.log
```
# PYTHON
```
python3 -m get_sd_ou.app
```
