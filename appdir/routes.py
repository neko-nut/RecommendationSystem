import datetime
import json
import math
import re
import time
from math import sqrt

import pandas as pd
from flask import request, jsonify
from influxdb import InfluxDBClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sshtunnel import SSHTunnelForwarder

import appdir.porter as porter
from appdir import application
from appdir.config import Config
from appdir.models import Asset, User

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
session = Session()

server1 = SSHTunnelForwarder(
    ('101.32.220.109', 22),
    ssh_username="root",
    ssh_password="F0G7oFpfYofirZPTl79H",
    remote_bind_address=('127.0.0.1', 8086)
)
server1.start()

client = InfluxDBClient('127.0.0.1', str(server1.local_bind_port), 'root', 'root', 'apex')

with open(Config.stopwords, 'r') as f:
    stopwords = set(f.read().split())

actions = {}
actions_user = {}
search = {}
assets_all = []
assets_now = []
popularity = {}
popularity_user = {}

area_min = 2147483647
area_max = 0
price_min = 2147483647
price_max = 0
year_min = 2147483647
year_max = 0
p = porter.PorterStemmer()
cache = {}
n = {}


@application.route('/')
def init():
    getaction()
    getasset()
    getfavorite()
    return jsonify({
        "code": 200,
        "msg": "OK"
    })


@application.route('/getaction')
def getaction():
    global actions
    global actions_user
    query = "SELECT * FROM browse WHERE time >= " + str(time.time_ns() - 31536000000000000)
    result = client.query(query)
    for table in result:
        for record in table:
            if record['user'] not in actions_user:
                actions_user[record['user']] = {}
            if record['asset'] not in actions_user[record['user']]:
                actions_user[record['user']][record['asset']] = 0
            actions_user[record['user']][record['asset']] = actions_user[record['user']][record['asset']] + record[
                'duration']
            if record['asset'] not in actions:
                actions[record['asset']] = 0
            actions[record['asset']] = actions[record['asset']] + record['duration']

    global search
    query = "SELECT * FROM search WHERE time >= " + str(time.time_ns() - 31536000000000000)
    result = client.query(query)
    for table in result:
        for record in table:
            d = {'location': record['location'], 'info': record['info']}
            if record['user'] not in search:
                search[record['user']] = []
            search[record['user']].append(d)

    return jsonify({
        "code": 200,
        "msg": "OK"
    })


def wordsanalysis(words):
    index = {}
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
    return index


@application.route('/getasset')
def getasset():
    # assets
    global assets_all
    global assets_now
    assets = session.query(Asset).all()
    global area_min
    global area_max
    global price_min
    global price_max
    global year_min
    global year_max

    # BM25
    global cache
    global n
    l = len(assets)
    len_sum = 0
    k = 1
    b = 0.75
    doclen = {}
    print('success')

    for asset in assets:
        dic = {}
        dic['id'] = asset.asset_id
        dic['longitude'] = 200
        dic['latitude'] = 200
        dic['region'] = ''
        dic['subregion'] = ''
        dic['street'] = ''
        if 'coordinates' in asset.asset_location['features'][0]['geometry']:
            dic['longitude'] = asset.asset_location['features'][0]['geometry']['coordinates'][0]
            dic['latitude'] = asset.asset_location['features'][0]['geometry']['coordinates'][1]
        if 'region' in asset.asset_location['features'][0]['properties']:
            dic['region'] = asset.asset_location['features'][0]['properties']['region']
        if 'subregion' in asset.asset_location['features'][0]['properties']:
            dic['subregion'] = asset.asset_location['features'][0]['properties']['subregion']
        if 'street' in asset.asset_location['features'][0]['properties']:
            dic['street'] = asset.asset_location['features'][0]['properties']['street']

        dic['type'] = 6
        dic['area'] = 0
        dic['price'] = 0
        dic['room'] = 0
        dic['bathroom'] = 0
        dic['garage'] = 0
        dic['year'] = 0

        if 'type' in asset.asset_info:
            dic['type'] = int(asset.asset_info['type'])

        if 'area' in asset.asset_info:
            if int(asset.asset_info['area']) > area_max:
                area_max = int(asset.asset_info['area'])
            elif int(asset.asset_info['area']) < area_min:
                area_min = int(asset.asset_info['area'])
            dic['area'] = int(asset.asset_info['area'])

        if 'price' in asset.asset_info:
            if int(asset.asset_info['price']) > price_max:
                price_max = int(asset.asset_info['price'])
            elif int(asset.asset_info['price']) < price_min:
                price_min = int(asset.asset_info['price'])
            dic['price'] = asset.asset_info['price']

        if 'year_built' in asset.asset_info:
            if int(asset.asset_info['year_built']) > year_max:
                year_max = int(asset.asset_info['year_built'])
            elif int(asset.asset_info['year_built']) < year_min:
                year_min = int(asset.asset_info['year_built'])
            dic['year'] = int(asset.asset_info['year_built'])

        if 'room' in asset.asset_info:
            dic['room'] = int(asset.asset_info['room'])

        if 'bathroom' in asset.asset_info:
            dic['bathroom'] = int(asset.asset_info['bathroom'])

        if 'garage' in asset.asset_info:
            dic['garage'] = int(asset.asset_info['garage'])

        assets_all.append(dic)

        if asset.asset_status == 1:
            dic['details'] = []
            dic['title'] = []
            if 'details' in asset.asset_info:
                words = asset.asset_info['details'].lower().split()
                words = re.split('[^a-z0-9]', str(words))
                words = [x for x in words if x != '']
                len_sum = len_sum + len(words)
                doclen[asset] = len(words)
                dic['details'] = wordsanalysis(words)

            words = asset.asset_title.lower().split()
            words = re.split('[^a-z0-9]', str(words))
            words = [x for x in words if x != '']
            dic['title'] = wordsanalysis(words)
            assets_now.append(dic)
    avg_doclen = len_sum / l
    print('success')

    for asset in assets_now:
        if len(dic['details']) > 0:
            for t in dic['details']:
                fi = dic['details'][t]
                dic['details'][t] = (fi * (1 + k) / fi + k * ((1 - b) + (b * doclen[asset]) / avg_doclen)) * math.log(
                    ((l - n[t] + 0.5) / (n[t] + 0.5)), 2)
            for t in dic['title']:
                fi = dic['title'][t]
                dic['title'][t] = (fi * (1 + k) / fi + k * ((1 - b) + (b * doclen[asset]) / avg_doclen)) * math.log(
                    ((l - n[t] + 0.5) / (n[t] + 0.5)), 2)

    return jsonify({
        "code": 200,
        "msg": "OK"
    })


@application.route('/getfavorite')
def getfavorite():
    global popularity
    global popularity_user
    users = session.query(User).all()
    for user in users:
        for asset in user.user_favorites:
            if asset not in popularity:
                popularity[asset] = 0
            popularity[asset] = popularity[asset] + 1
        popularity_user[user.user_id] = user.user_favorites
    return jsonify({
        "code": 200,
        "msg": "OK"
    })


@application.route('/getpopularity')
def getpopularity():
    pop_nom = minmax(popularity)
    act_nom = minmax(actions)
    result = {}
    for asset in assets_now:
        p = 0
        a = 0
        if asset['id'] in pop_nom:
            p = pop_nom[asset['id']] + 0.01
        if asset['id'] in act_nom:
            a = (act_nom[asset['id']] + 0.01) * 3
        result[asset['id']] = p + a
    res_sort = sorted(result, key=result.get, reverse=True)
    return jsonify({
        "code": 200,
        "msg": "OK",
        "data": res_sort
    })


def minmax(dic):
    max = 0
    min = 2147483647
    for n in dic:
        if dic[n] > max:
            max = dic[n]
        if dic[n] < min:
            min = dic[n]
    if min == max:
        for n in dic:
            dic[n] = 1
    else:
        for n in dic:
            dic[n] = (dic[n] - min) / (max - min)
    return dic


@application.route('/retrieval', methods=['GET', 'POST'])
def retrieval():
    result = {}
    # get data
    user = -1
    location = None
    info = None
    if request.method == "POST":
        user = request.form.get('user')
        location = json.loads(request.form.get('location'))
        info = json.loads(request.form.get('info'))

    # analysis details
    if info is not None:
        if len(info['details']) > 0:
            index = []
            info['details'] = info['details'].strip().lower()
            info['details'] = re.split('[^a-z|0-9]', str(info['details']))
            info['details'] = [x for x in info['details'] if x != '']
            for w in info['details']:
                if w not in stopwords:
                    if w not in cache:
                        cache[w] = p.stem(w)
                    w = cache[w]
                    index.append(w)

    for asset in assets_now:
        if location['subregion'] == asset['subregion']:
            result[asset['id']] = {}

            # calculate the z score for distance
            dis_sum = 0
            result[asset['id']]['distance'] = sqrt(pow((location['longitude'] - asset['longitude']), 2) +
                                                   pow((location['latitude'] - asset['latitude']), 2))
            dis_sum = dis_sum + result[asset['id']]['distance']

            # matching degree
            result[asset['id']]['match'] = 0
            if len(location['subregion']) > 0:
                if location['subregion'] != asset['subregion']:
                    result[asset['id']]['match'] = result[asset['id']]['match'] + 1
            if len(location['street']) > 0:
                if location['street'] != asset['street']:
                    result[asset['id']]['match'] = result[asset['id']]['match'] + 1

            if info is not None:
                # matching degree
                if info['type'] != 6:
                    if info['type'] != asset.info['type']:
                        result[asset['id']]['match'] = result[asset['id']]['match'] + 1

                if info['area'][1] > area_max:
                    info['area'][1] = area_max
                elif info['area'][0] < area_min:
                    info['area'][0] = area_min
                if asset['area'] > info['area'][1] or asset['area'] < info['area'][0]:
                    result[asset['id']]['match'] = result[asset['id']]['match'] + 1

                if info['price'][1] > price_max:
                    info['price'][1] = price_max
                elif info['price'][0] < price_min:
                    info['price'][0] = price_min
                if asset['price'] > info['price'][1] or asset['price'] < info['price'][0]:
                    result[asset['id']]['match'] = result[asset['id']]['match'] + 1

                if info['year'][1] > year_max:
                    info['year'][1] = year_max
                elif info['year'][0] < year_min:
                    info['year'][0] = year_min
                if asset['year'] > info['year'][1] or asset['year'] < info['year'][0]:
                    result[asset['id']]['match'] = result[asset['id']]['match'] + 1

                if info['room'] < info['room']:
                    result[asset['id']]['match'] = result[asset['id']]['match'] + 1
                if info['bathroom'] < info['bathroom']:
                    result[asset['id']]['match'] = result[asset['id']]['match'] + 1
                if info['garage'] < info['garage']:
                    result[asset['id']]['match'] = result[asset['id']]['match'] + 1

                if len(info['details']) > 0:
                    res = 0
                    for q in index:
                        if q in asset['details']:
                            res = res + asset['details'][q]
                    result[asset['id']]['details'] = res
                print(result[asset['id']])

    # if user == -1:
    #     ## TODO

    return "success"


@application.route('/addaction')
def addaction():
    json_body = [
        {
            "measurement": "browse",
            "tags": {
                "user": '1',
                "asset": '4'
            },
            "fields": {
                "duration": 101,
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
                        'subregion': str(int(df.loc[i, 'city'])),
                        'street': ''
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
            Asset(revision=version, asset_id=id, asset_title=title, asset_location=location, asset_info=info,
                  asset_open=datetime.datetime.now(), asset_status=1))
    session.commit()
    return "success"
