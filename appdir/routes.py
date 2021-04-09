import math
import re
from math import sqrt
import datetime
import json


import pandas as pd
from flask import request, jsonify
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sshtunnel import SSHTunnelForwarder
from influxdb import InfluxDBClient

from appdir import application
from appdir.config import Config
from appdir.models import Asset
import appdir.porter as porter

server = SSHTunnelForwarder(
    ('101.32.220.109', 22),
    ssh_username="root",
    ssh_password="F0G7oFpfYofirZPTl79H",
    remote_bind_address=('127.0.0.1', 3306)
)

server.start()
local_port = str(server.local_bind_port)
engine = create_engine(
    'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8mb4'.format(
        "root",
        "F0G7oFpfYofirZPTl79H",
        "127.0.0.1",
        local_port,
        "apex")
)
Session = sessionmaker(bind=engine)
global session
session = Session()

server1 = SSHTunnelForwarder(
    ('101.32.220.109', 22),
    ssh_username="root",
    ssh_password="F0G7oFpfYofirZPTl79H",
    remote_bind_address=('127.0.0.1', 8086)
)
server1.start()

client = InfluxDBClient('127.0.0.1', str(server1.local_bind_port), 'root', 'root', 'example')
client.create_database('apex')

with open(Config.stopwords, 'r') as f:
    stopwords = set(f.read().split())

global assets
global actions
global area_sum
global price_sum
global year_sum
global area_min
global area_max
global price_min
global price_max
global year_min
global year_max
global area_s_min
global area_s_max
global price_s_min
global price_s_max
global year_s_min
global year_s_max
global area_avg
global price_avg
global year_avg
global area_s
global price_s
global year_s
global cache


@application.route('/getaction')
def getaction():
    result = client.query('select * from access_log;')

    for table in result:
        print(table)
        print(table[0])
        for record in table:
            print(record)
        global actions
        actions = table
    return str(result)


@application.route('/accessrate')
def accessrate():
    result = client.query('select * from access_log;')

    for table in result:
        print(table)
        print(table[0])
        for record in table:
            print(record)
        global actions
        actions = table
    return str(result)



@application.route('/getasset')
def getasset():
    global assets
    assets = list(session.query(Asset).filter(Asset.asset_status == 1).all())
    global area_sum
    global price_sum
    global year_sum
    area_sum = 0
    price_sum = 0
    year_sum = 0
    global area_min
    global area_max
    global price_min
    global price_max
    global year_min
    global year_max
    area_min = 2147483647
    area_max = 0
    price_min = 2147483647
    price_max = 0
    year_min = 2147483647
    year_max = 0
    l = len(assets)
    print('success')
    global cache
    p = porter.PorterStemmer()
    cache = {}
    len_sum = 0
    k = 1
    b = 0.75
    doclen = {}
    n = {}
    index = {}
    for d in assets:
        area_sum = area_sum + d.asset_info['area']
        price_sum = price_sum + d.asset_info['price']
        year_sum = year_sum + d.asset_info['year_built']
        if d.asset_info['area'] > area_max:
            area_max = d.asset_info['area']
        if d.asset_info['area'] < area_min:
            area_min = d.asset_info['area']
        if d.asset_info['price'] > price_max:
            price_max = d.asset_info['price']
        if d.asset_info['price'] < price_min:
            price_min = d.asset_info['price']
        if d.asset_info['year_built'] > year_max:
            year_max = d.asset_info['year_built']
        if d.asset_info['year_built'] < year_min:
            year_min = d.asset_info['year_built']
        words = d.asset_info['details'].lower().split()
        words = re.split('[^a-z0-9]', str(words))
        words = [x for x in words if x != '']
        len_sum = len_sum + len(words)
        doclen[d] = len(words)
        for w in words:
            if w not in stopwords:
                if w not in cache:
                    cache[w] = p.stem(w)
                w = cache[w]
                if w not in index:
                    index[w] = 1
                    if w not in n:
                        n[w] = 1
                    else:
                        n[w] = n[w] + 1
                else:
                    index[w] = index[w] + 1
        d.asset_info['details'] = index
        words = d.asset_title.lower().split()
        words = re.split('[^a-z0-9]', str(words))
        words = [x for x in words if x != '']
        for w in words:
            if w not in stopwords:
                if w not in cache:
                    cache[w] = p.stem(w)
                w = cache[w]
                if w not in index:
                    index[w] = 1
                    if w not in n:
                        n[w] = 1
                    else:
                        n[w] = n[w] + 1
                else:
                    index[w] = index[w] + 1
        d.asset_title = index
    avg_doclen = len_sum / l
    print('success')
    global area_avg
    global price_avg
    global year_avg
    area_avg = area_sum / l
    price_avg = price_sum / l
    year_avg = year_sum / l
    global area_s
    global price_s
    global year_s
    area_s = 0
    price_s = 0
    year_s = 0
    for d in assets:
        area_s = area_s + pow((d.asset_info['area'] - area_avg), 2)
        price_s = price_s + pow((d.asset_info['price'] - price_avg), 2)
        year_s = year_s + pow((d.asset_info['year_built'] - year_avg), 2)
        for t in d.asset_info['details']:
            fi = d.asset_info['details'][t]
            d.asset_info['details'][t] = (fi * (1 + k) / fi + k * ((1 - b) + (b * doclen[d]) / avg_doclen)) * math.log(((l - n[t] + 0.5) / (n[t] + 0.5)), 2)
        for t in d.asset_title:
            fi = d.asset_title[t]
            d.asset_title[t] = (fi * (1 + k) / fi + k * ((1 - b) + (b * doclen[d]) / avg_doclen)) * math.log(((l - n[t] + 0.5) / (n[t] + 0.5)), 2)
    print(area_s)
    area_s = sqrt(area_s / l)
    price_s = sqrt(price_s / l)
    year_s = sqrt(year_s / l)
    print(area_s)
    print('success')
    global area_s_min
    global area_s_max
    global price_s_min
    global price_s_max
    global year_s_min
    global year_s_max
    area_s_min = 2147483647
    area_s_max = 0
    price_s_min = 2147483647
    price_s_max = 0
    year_s_min = 2147483647
    year_s_max = 0
    for d in assets:
        d.asset_info['area'] = z(d.asset_info['area'], area_avg, area_s)
        d.asset_info['price'] = z(d.asset_info['price'], price_avg, price_s)
        d.asset_info['year_built'] = z(d.asset_info['year_built'], year_avg, year_s)
        if d.asset_info['area'] > area_s_max:
            area_s_max = d.asset_info['area']
        elif d.asset_info['area'] < area_s_min:
            area_s_min = d.asset_info['area']
        if d.asset_info['price'] > price_s_max:
            price_s_max = d.asset_info['price']
        elif d.asset_info['price'] < price_s_min:
            price_s_min = d.asset_info['price']
        if d.asset_info['year_built'] > year_s_max:
            year_s_max = d.asset_info['year_built']
        elif d.asset_info['year_built'] < year_s_min:
            year_s_min = d.asset_info['year_built']
    for d in assets:
        print(area_s_min, area_s_max)
        d.asset_info['area'] = (d.asset_info['area'] - area_s_min) / (area_s_max - area_s_min)
        d.asset_info['price'] = (d.asset_info['price'] - price_s_min) / (price_s_max - price_s_min)
        d.asset_info['year_built'] = (d.asset_info['year_built'] - year_s_min) / (year_s_max - year_s_min)
        if d.asset_info['area'] > 0.1:
            print(d.asset_info['area'])
    return 'success'


@application.route('/retrieval', methods=['GET', 'POST'])
def retrieval():
    result = {}
    user = -1
    location = None
    info = None
    dis_sum = 0
    if request.method == "POST":
        user = request.form.get('user')
        location = json.loads(request.form.get('location'))
        info = json.loads(request.form.get('info'))
    print(info)
    if len(info['details']) > 0:
        info['details'] = info['details'].strip().lower()
        info['details'] = re.split('[^a-z|0-9]', str(info['details']))
        info['details'] = [x for x in info['details'] if x != '']
        index = []
        p = porter.PorterStemmer()
    for w in info['details']:
        if w not in stopwords:
            if w not in cache:
                cache[w] = p.stem(w)
            w = cache[w]
            index.append(w)
    for d in assets:
        result[d.asset_id] = {}
        if location['features'][0]['properties']['region'] == d.asset_location['features'][0]['properties']['region']:
            result[d.asset_id]['distance'] = sqrt(pow((location['features'][0]['geometry']['coordinates'][0] - d.asset_location['features'][0]['geometry']['coordinates'][0]), 2) + pow((location['features'][0]['geometry']['coordinates'][1] - d.asset_location['features'][0]['geometry']['coordinates'][1]), 2))
            dis_sum = dis_sum + result[d.asset_id]['distance']
            result[d.asset_id]['address'] = 0
            if location['features'][0]['properties']['subregion'] != d.asset_location['features'][0]['properties']['subregion']:
                result[d.asset_id]['address'] = result[d.asset_id]['address'] + 1
            if len(location['features'][0]['properties']['street']) > 0:
                if location['features'][0]['properties']['street'] != d.asset_location['features'][0]['properties']['street']:
                    result[d.asset_id]['address'] = result[d.asset_id]['address'] + 1
        if info is not None:
            if info['type'] != -1:
                if info['type'] == d.info['type']:
                    result[d.asset_id]['type'] = 1
            if info['area'][1] > area_max:
                info['area'][1] = area_max
            elif info['area'][0] < area_min:
                info['area'][0] = area_min
            if d.asset_info['area'] > area_max or d.asset_info['area'] < area_min:
                result[d.asset_id]['area'] = (
                            z(info['area'][1], area_avg, area_s) - z(info['area'][0], area_avg, area_s))
            if info['price'][1] > price_max:
                info['price'][1] = price_max
            elif info['price'][0] < price_min:
                info['price'][0] = price_min
            if d.asset_info['price'] > price_max or d.asset_info['price'] < price_min:
                result[d.asset_id]['price'] = (
                            z(info['price'][1], price_avg, price_s) - z(info['price'][0], price_avg, price_s))
            if info['year_built'][1] > year_max:
                info['year_built'][1] = year_max
            elif info['year_built'][0] < year_min:
                info['year_built'][0] = year_min
            if d.asset_info['year_built'] > year_max or d.asset_info['year_built'] < year_min:
                result[d.asset_id]['year_built'] = (
                            z(info['year_built'][1], year_avg, year_s) - z(info['year_built'][0], year_avg, year_s))

            if info['room'] < d.asset_info['room']:
                result[d.asset_id]['room'] = 1
            if info['bathroom'] < d.asset_info['bathroom']:
                result[d.asset_id]['bathroom'] = 1
            if info['garage'] < d.asset_info['garage']:
                result[d.asset_id]['garage'] = 1
            if len(info['details']) > 0:
                result = 0
                for q in index:
                    if q in d.asset_info['details']:
                        result = result + d.asset_info['details'][q]
                result[d.asset_id]['details'] = result
            print(result[d.asset_id])

    # if user == -1:
    #     # TODO

    return "success"


def z(i, avg, s):
    return (i - avg) / s


@application.route('/addaction')
def addaction():
    json_body = [
        {
            "measurement": "access_log",
            "time": "2021-03-10T23:00:00Z",
            "fields": {
                "user": 1,
                "asset": 1,
                "second": 100
            }
        }
    ]

    client.write_points(json_body)
    return "success"


@application.route('/add')
def add():
    attributes = [
        "bathroom",
        "bedroom",
        "size",
        "garage",
        "latitude",
        "longitude",
        "pool",
        "city",
        "county",
        "yard",
        "year",
        "price"
    ]

    df = pd.read_csv(Config.Data, encoding='unicode_escape')
    df.columns = attributes
    f = [
        "bathroom",
        "bedroom",
        "garage",
        "pool",
        "yard",
    ]
    df[f] = df[f].fillna(0)
    df.dropna(axis=0, how='any', thresh=None, subset=None, inplace=True)
    j = 3
    for i in df.index:
        version = 1
        id = j
        j = j + 1
        location = {
            'type': "FeatureCollection",
            'features': [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        'coordinates': [df.loc[i, 'longitude'] / 10000000, df.loc[i, 'latitude'] / 10000000],
                    },
                    "properties": {
                        "name": "New York",
                        "description": 'New York',
                        'region': str(int(df.loc[i, 'county'])),
                        'subregion': str(int(df.loc[i, 'city']))
                    }
                }
            ]
        }
        info = {
            'bathroom': int(df.loc[i, 'bathroom']),
            'room': int(df.loc[i, 'bedroom']),
            'area': int(df.loc[i, 'size']),
            'year_built': int(df.loc[i, 'year']),
            'price': int(df.loc[i, 'price']),
            'garage': int(df.loc[i, 'garage'])
        }

        if i < 5000:
            info['type'] = 1
            title = 'house'
            if i < 1234:
                title = 'a second hand ' + title
        else:
            info['type'] = 0
            title = 'apartment'
            if i < 7654:
                title = 'a new ' + title
        info['details'] = ''
        if df.loc[i, 'garage'] > 0:
            title = title + ' with ' + str(int(df.loc[i, 'garage'])) + ' garden'
        if df.loc[i, 'pool'] > 0:
            info['details'] = info['details'] + 'have ' + str(df.loc[i, 'pool']) + 'swimming pool '
        if df.loc[i, 'yard'] > 0:
            info['details'] = info['details'] + 'have a ' + str(df.loc[i, 'yard']) + 'square feet yard '
        session.add(
            Asset(revision=version, asset_id=id, asset_title=title, asset_location=location, asset_info=info, asset_open=datetime.datetime.now(), asset_status=1))
    session.commit()
    return "success"
