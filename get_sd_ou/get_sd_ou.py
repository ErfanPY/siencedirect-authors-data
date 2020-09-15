import requests

search_url = 'https://www.sciencedirect.com/search?qs=art&date={}'
headers = {
        'Accept' : 'application/json',
        'user-agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:80.0) Gecko/20100101 Firefox/80.0'
        }
history = {}
start_year = 2010
end_year = 2020

for year in range(end_year, start_year, -1):
    search = requests.get(search_url.format(year), headers=headers)
    soup = bs(search.content)
    soup.find_all('a', )
