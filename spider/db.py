# -*- coding:utf-8 -*-
__author__ = 'victor'

import sys
reload(sys)
sys.setdefaultencoding('utf-8')


def get_mysql_config():

    return {
        'host': 'localhost:3306',
        'database': 'iwjw',
        'user': 'root',
        'password': '',
    }


def update_district(db, did, dname, type, hids):

    if dname and not db.get('select * from district where districtId=%s', did):
        db.execute('insert into district (districtId, districtName) values (%s, %s);', did, dname)
    for hid in hids:
        if not db.get('select * from house where houseId=%s', hid):
            db.execute('insert into house (houseId, districtId, type) values (%s, %s, %s);', hid, did, type)