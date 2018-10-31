import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

# database connect setting
SERVER = 'soshenkov.ru'
DBNAME = 'main'
USER = 'main'
PASS = 'B2C-pPN-7L2-FcX'


def ats_max_hour(period):
    URL: str = 'https://www.atsenergo.ru/js-data'
    DATA = 'results/market/calcfacthour'

    r = requests.post(URL, data={'data': DATA, 'id': period})

    _links = []
    soup = BeautifulSoup(r.content, 'lxml')

    divs = soup.find_all('div', {'class': 'xml-data-row'})
    for div in divs:
        _links.append([div.find('div', {'class': 'header'}).text, div.find('a').get('href')])
    return _links


def read_xls(region, _link):
    xls = pd.read_excel(_link, sheet_name=0, skiprows=7, names=['dt', 'hour'])
    xls.hour = xls.hour - 1
    xls['region'] = region
    xls['dt'] = pd.to_datetime(xls['dt'])

    engine = create_engine('postgresql://' + USER + ':' + PASS + '@' + SERVER + '/' + DBNAME)

    xls.to_sql('max_gp_hour', con=engine, if_exists='append', index=False)


for year in range(2018, 2019):
    for i in range(1, 10):
        links = ats_max_hour('%02d' % i + str(year))
        print('%02d' % i + str(year))
        for link in links:
            read_xls(link[0], link[1])
            print(link[0])
