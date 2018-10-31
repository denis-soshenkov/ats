import requests
from bs4 import BeautifulSoup
import re
from sqlalchemy import create_engine
import postgresql
import time


BASE_URL = 'https://energybase.ru'
REVERSE_GEO_URL = 'https://nominatim.openstreetmap.org/reverse?format=xml&addressdetails=1&'
INDEX = '/substation/index?page='
ps_links = []
pq_str = 'main:B2C-pPN-7L2-FcX@soshenkov.ru/main'
engine = create_engine('postgresql://' + pq_str)


def reverse_region(lat, lon):
    s = requests.session()
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36'}
    r = s.get(REVERSE_GEO_URL + 'lat=' + str(lat) + '&lon=' + str(lon), headers=headers)
    s.close()

    soup = BeautifulSoup(r.content, 'xml')
    try:
        return soup.find('state').text
    except AttributeError:
        return 'Unknown'


def last_page():
    # Определение последней страницы с перечнем подстанций
    s = requests.session()
    r = s.get(BASE_URL + '/substation/index?page=1')
    s.close()

    soup = BeautifulSoup(r.content, 'html.parser')
    pagination = soup.find('li', {'class': 'last'})

    return int(pagination.a['data-page']) + 1


def list_of_ps(start_page, stop_page):
    # Список подстанций
    temp = []
    for i in range(start_page, stop_page + 1):
        s = requests.session()
        r = s.get(BASE_URL + INDEX + str(i))
        s.close()

        soup = BeautifulSoup(r.content, 'html.parser')
        links = soup.find_all('a', {'class': 'icon substation'})
        for link in links:
            temp.append(BASE_URL + link.get('href'))
        print('Страница %d из %d' % (i, stop_page))
    return temp, len(temp)


def ps_page(link):
    # Открываем страницу с подстанцией и ищем всю необходимую информацию
    s = requests.session()
    r = s.get(link)
    s.close()

    soup = BeautifulSoup(r.content, 'html.parser')
    section = soup.find('section', {'class': 'model-details substation'})

    psName = section.header.h1.text
    psSO = section.header.h3.a.text

    try:
        psU = int(re.findall(r'^\d{,3}', section.section.find('small', text='кВ').find_parent('div').text)[0])
    except:
        psU = 0

    try:
        psRegion = section.find('section', {'class': 'contacts'}).find('meta', {'itemprop': 'streetAddress'})['content']
    except:
        psRegion = 'Unknown'

    try:
        psLatitude = float(re.findall(r'\d{1,3}.\d{1,9}', section.find_all('a', href='#yandex-map')[0].text)[0])
        psLongitude = float(re.findall(r'\d{1,3}.\d{1,9}', section.find_all('a', href='#yandex-map')[1].text)[0])
    except:
        psLatitude = 0
        psLongitude = 0

    return [psName, psSO, psU, psRegion, psLatitude, psLongitude, link]


def ps_into_db(arr):
    connection.execute('INSERT INTO public.substations (psname, psso, psu, psregion, pslatitude, pslongitude, '
                       'psurl) VALUES (\'%s\', \'%s\', %d, \'%s\', %f, %f, \'%s\')' % (
                           arr[0], arr[1], arr[2], arr[3][:45], arr[4],
                           arr[5], arr[6]))


# ps, count = list_of_ps(701, 780)
#
# for link in ps[-712:]:
#     print(count, link)
#     ps_into_db(ps_page(link))
#     count -= 1

with postgresql.open('pq://' + pq_str) as db:
    arr = db.query("select id, pslatitude, pslongitude from substations where psregion = 'Unknown';")
    update = db.prepare("Update substations set psregion = $2 where id = $1")
    for row in arr:
        time.sleep(1)
        region = reverse_region(row['pslatitude'], row['pslongitude'])
        print(row['id'], region)
        update(row['id'], region)

# connection.close()
