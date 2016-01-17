# -*- coding:utf-8 -*-
__author__ = 'victor'

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import os
import re
import codecs
import logging
import torndb
from bs4 import BeautifulSoup

import processor as ps
from dianping.dianping.spiders import dbutil
import util
from master import Master
from fetcher import Fetcher

district = re.compile(r'(.+)p\d{1,2}.html')
estate = re.compile(r'/estate/([^/]+)/')
unit = re.compile(u'([\d\.]+)万/平')
room = re.compile(u'(\d)室')
room_size = re.compile(u'([\d\.]+)[ ]*m²')
number = re.compile(r'(\d+)')
estate_addr = re.compile(u'：(.+)')
housetypes = re.compile(u'(.)居')
lon = re.compile(r'data-lon="([\d\.]+)')
lat = re.compile(r'data-lat="([\d\.]+)')


logging.getLogger('iwjw').handlers = []
logger = logging.getLogger('iwjw')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(name)-12s %(asctime)s %(levelname)-8s %(message)s', '%a, %d %b %Y %H:%M:%S',)
stream_handler = logging.StreamHandler(sys.stderr)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


def fetch_list():

    print 'sale_list'
    master = Master(rest_period=5, result_model='html', result_dir='../iwjw/sale_list')
    fetcher = Fetcher(processor=ps.ProcessorIwjw())
    master.add_fetchers(fetcher)
    urls = [line.split('#')[0].strip() for line in codecs.open('../district.id')]
    urls = map(lambda x: 'http://www.iwjw.com/sale/shanghai/%sp1/' % x, urls)
    master.start(urls)

    print 'rent_list'
    master = Master(rest_period=5, result_model='html', result_dir='../iwjw/rent_list')
    fetcher = Fetcher(processor=ps.ProcessorIwjw())
    master.add_fetchers(fetcher)
    urls = [line.split('#')[0].strip() for line in codecs.open('../district.id')]
    urls = map(lambda x: 'http://www.iwjw.com/chuzu/shanghai/%sp1/' % x, urls)
    master.start(urls)


def get_houses(dir, type='sale'):

    global district, logger
    house = re.compile(r'http://www.iwjw.com/%s/([^/]+)/\?from' % type)
    district_re = re.compile(r'searchName="(.+)"')
    type_mapping = {
        'sale': 1,
        'chuzu': 0
    }
    for f in os.listdir(dir):
        try:
            district_id = district.findall(f)[0]
        except:
            logger.error('district id failure#%s' % f)
            continue
        with codecs.open(os.path.join(dir, f), encoding='utf-8') as content:
            content = content.read()
            try:
                district_name = district_re.findall(content)[0].strip()
            except:
                district_name = None
                logger.error('district name failure#%s' % district_id)
            house_ids = set(house.findall(content))
            database = torndb.Connection(**dbutil.get_mysql_config())
            dbutil.update_district(database, district_id, district_name, type_mapping.get(type), house_ids)
            database.close()
            for id in house_ids:
                yield 'http://www.iwjw.com/%s/%s/' % (type, id)


def fetch_house():

    # print 'sales'
    # master = Master(rest_period=5, result_model='html', result_dir='../iwjw/sale')
    # fetcher = Fetcher(processor=ps.Processor_hn())
    # master.add_fetchers(fetcher)
    # sales = list(get_houses('../iwjw/sale_list', 'sale'))
    # master.start(sales)

    print 'rent'
    master = Master(rest_period=5, result_model='html', result_dir='../iwjw/rent')
    fetcher = Fetcher(processor=ps.Processor_hn())
    master.add_fetchers(fetcher)
    rents = list(get_houses('../iwjw/rent_list', 'chuzu'))
    master.start(rents)


def fetch_house_from_db():

    print 'sales'
    existed = set([f.replace('.html', '') for f in os.listdir('../iwjw/sale')])
    master = Master(rest_period=5, result_model='html', result_dir='../iwjw/sale')
    fetcher = Fetcher(processor=ps.Processor_hn())
    master.add_fetchers(fetcher)
    database = torndb.Connection(**dbutil.get_mysql_config())
    sale_list = database.query('select houseId from house where type=1;')
    sale_list = [result.houseId for result in sale_list if not result.houseId in existed]
    sale_list = ['http://www.iwjw.com/sale/%s/' % hid for hid in sale_list]
    master.start(sale_list)
    database.close()


def process1house(database, houseId, path_template):

    global estate, unit, room, room_size, logger
    path = path_template % houseId
    with codecs.open(path, encoding='utf-8') as f:
        html = f.read()
        soup = BeautifulSoup(html)
        try:
            estate_id = estate.findall(html)[0].strip()
        except Exception, e:
            estate_id = 'unknown'
            logger.Exception(houseId, e)
        total_price = float(soup.find_all(name='i', attrs={'class': 'price'})[0].string)
        num_room = int(room.findall(html)[0])
        size = float(room_size.findall(html)[0])
        unit_price = round(total_price/size, 4)

    name_list = ['communityId', 'totalPrice', 'unitPrice', 'room', 'size', 'houseId']
    value_list = [estate_id, total_price, unit_price, num_room, size, houseId]
    database.execute('update house set communityId=%s, totalPrice=%s, unitPrice=%s, room=%s, size=%s '
                     'where houseId=%s', *value_list)


def process_houses():

    database = torndb.Connection(**dbutil.get_mysql_config())
    template = {
        0: '../iwjw/rent/%s.html',
        1: '../iwjw/sale/%s.html'
    }
    for result in database.query('select type, houseId from house;'):
        type, houseId = int(result.type), result.houseId
        try:
            process1house(database, houseId, template.get(type))
            logger.info('processed#%s' % houseId)
        except Exception, e:
            continue
    database.close()


def fetch_estate():

    database = torndb.Connection(**dbutil.get_mysql_config())
    urls = []
    for result in database.query('select distinct communityId from house'):
        estate_id = result.communityId
        urls.append('http://www.iwjw.com/estate/%s/' % estate_id)
    master = Master(rest_period=5, result_model='html', result_dir='../iwjw/estate')
    fetcher = Fetcher(processor=ps.Processor_hn())
    master.add_fetchers(fetcher)
    master.start(urls)


def process1estate(database, community_id, path_template):

    global number, estate_addr, housetypes, lon, lat
    path = path_template % community_id
    with codecs.open(path, encoding='utf-8') as f:
        html = f.read()
        soup = BeautifulSoup(html)
        name = soup.find(name='div', attrs={'class': 'titles'}).find('p', attrs={'class': 'h3'})['title']
        etype, hc, year, park_size, addr, intro, house_type = None, None, None, None, None, None, None
        blat, blon, glat, glon = 0, 0, 0, 0
        try:
            for span in soup.find(name='div', attrs={'class': 'det'}).find_all('span'):
                if u'房屋类型' in str(span):
                    etype = span.find_all('i')[-1].get_text()
                elif u'房屋总数' in str(span):
                    numbers = number.findall(span.get_text())
                    hc = numbers[0] if numbers else 0
                elif u'建造年代' in str(span):
                    numbers = number.findall(span.get_text())
                    year = numbers[0]if numbers else 0
                elif u'停  车  位' in str(span):
                    numbers = number.findall(span.get_text())
                    park_size = numbers[0] if numbers else 0
                elif u'小区地址' in str(span):
                    addr = estate_addr.findall(span.get_text())[0]
        except Exception, e:
            logger.error('%s#content error#%s' % (community_id, e))
        try:
            extra = soup.find_all(name='p', attrs={'class': 'sbs'})
            intro = extra[0].span.get_text()
            if len(extra) > 1:
                house_type = '#'.join(housetypes.findall(extra[1].get_text()))
        except Exception, e:
            logger.error('%s#extra error#%s' % (community_id, e))
        try:
            blon, blat = round(float(lon.findall(html)[0]), 3), round(float(lat.findall(html)[0]), 3)
            glon, glat = map(lambda x: round(x, 3), util.bd_decrypt((blon, blat)))
        except:
            logger.error('%s#titude error' % community_id)
    if database.get('select * from community where communityId=%s', community_id):
        database.execute('update community set name=%s, type=%s, totalHouses=%s, year=%s, parkSize=%s, '
                         'address=%s, intro=%s, houseType=%s, blon=%s, blat=%s, glon=%s, glat=%s where communityId=%s',
                         name, etype, hc, year, park_size, addr,
                         intro, house_type, blon, blat, glon, glat, community_id)
    else:
        database.execute('insert into community (communityId, name, type, totalHouses, year, parkSize, address, intro, '
                         'houseType, blon, blat, glon, glat) '
                         'values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                         community_id, '大厦', etype, hc, year, park_size, addr,
                         intro, house_type, blon, blat, glon, glat)


def process_estates():

    database = torndb.Connection(**dbutil.get_mysql_config())
    path_template = '../iwjw/estate/%s.html'
    eids = map(lambda x: x.communityId, database.query('select distinct communityId from house;'))
    for eid in eids:
        try:
            process1estate(database, eid, path_template)
        except Exception, e:
            logger.error('%s#%s' % (eid, e))
    database.close()

if __name__ == '__main__':

    print __file__

    process_estates()

    # fetch_estate()
    # fetch_house()
    # fetch_house_from_db()