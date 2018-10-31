import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import requests
from bs4 import BeautifulSoup

URL_PREFIX = 'https://www.atsenergo.ru/nreport?access=public&rname=big_nodes_prices_pub&rdate='
URL_SUFFIX = '_eur_big_nodes_prices_pub.xls'

# database connect setting
SERVER = 'soshenkov.ru'
DBNAME = 'main'
USER = 'main'
PASS = 'B2C-pPN-7L2-FcX'

engine = create_engine('postgresql://' + USER + ':' + PASS + '@' + SERVER + '/' + DBNAME)

# for YEAR in range(2016, 2019):
#     for MONTH in range(1, 13):
#         for DAY in range(1, 32):
#             try:
#                 with pd.ExcelFile('20160101_eur_big_nodes_prices_pub.xls') as xls:
#                     nodes_xls = pd.read_excel(xls, sheet_name='0', skiprows=2, usecols="A:E",
#                                               names=['node_id', 'node_name', 'node_u', 'region', 'price'])


YEAR = 2016
MONTH = 1
DAY = 1
HOUR = 1

date_str = '%04d' % YEAR + '%02d' % MONTH + '%02d' % DAY

r = requests.get(URL_PREFIX + date_str)
soup = BeautifulSoup(r.content, 'lxml')
# print(soup.prettify())
link = soup.find('a', text=date_str + URL_SUFFIX).get('href')

dt_string = datetime(YEAR, MONTH, DAY, HOUR)
print(link)

#

#
# nodes_db = pd.read_sql_table('nodes', engine, index_col='id')
# # nodes_temp = pd.merge(nodes_db, nodes_xls, how='outer', validate='one_to_one')
# print(nodes_xls.head())
# print(nodes_db.head())
# print(nodes_xls.dtypes)