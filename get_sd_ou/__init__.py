import queue
import threading
from urllib.parse import parse_qsl, unquote_plus, urljoin, urlparse

import requests
from bs4 import BeautifulSoup as bs

