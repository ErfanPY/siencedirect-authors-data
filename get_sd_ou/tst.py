import requests
from get_sd_ou.logger_util import init_logger
logger = init_logger()

from get_sd_ou.__init__ import *
dir()
import get_sd_ou.class_util as class_util
from get_sd_ou.class_util import Article

class_util.logger = logger
url = 'https://www.sciencedirect.com/science/article/pii/S0950423020305763#!'
headers  = {
        'Accept' : 'application/json',
        'user-agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/80.0'
        }
art = Article(url)
print(art.authors())