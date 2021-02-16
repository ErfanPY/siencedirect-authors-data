## Sciencedirect scraper :

[Sciencedirect](https://www.sciencedirect.com/) Authors scraper.

You can choose the search parameters (same as sciencedirect article search parameters) for the form in the index page.

Also you can goto database result search page and search for scraped article and authors data.

## REQUIREMENT

mysql

- Python packages in requirement.txt

## RUN

# Virtual environment

```
virtualenv venv
source ./venv/bin/activate
pip install -r ./requirement.txt
```

# MYSQL

```
mysql -uroot -p < ./db/scripts/sciencedirect_complete_clear.sql
```

# PYTHON

```
python3 -m get_sd_ou.app
```
