import datetime
import json
import math
import re
import time
from math import sqrt
import random

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

zip_code = {}
state = {}
with open(Config.zip, 'r') as f:
    line = f.readline()
    while len(line) > 0:
        line = re.split("[^a-z|A-Z|0-9| ]", str(line))
        line = [x for x in line if x != '']
        line = [x for x in line if x != ' ']
        state[line[4]] = line[3]
        line = f.readline()
    state['PR'] = 'Puerto Rico'
    state['VI'] = 'Virgin Islands'
    state['AE'] = 'Armed Forces – Europe'
    state['AA'] = 'Armed Forces – Americas'
    state['AP'] = 'Armed Forces – Pacific'
    state['AS'] = 'American Samoa'
    print(len(state))

with open(Config.zips, 'r') as load_f:
    load_dict = json.load(load_f)
    # print(load_dict)
    for zip in load_dict:
        if zip['country'] == "US":
            zip_code[zip['zip_code']] = {"country": zip['country'].split('[^a-z|A-Z| ]')[0], "subregion": zip['city'].split('[^a-z|A-Z| ]')[0], 'region': state[zip['state']].split('[^a-z|A-Z| ]')[0]}
    print(zip_code)


actions = {}
actions_user = {}
search = {}
assets_all = {}
assets_now = {}
popularity = {}
popularity_user = {}
popularity_value = {}

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
    getuserinfo()
    getpopularity()
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
            if int(record['user']) not in actions_user:
                actions_user[int(record['user'])] = {}
            if int(record['asset']) not in actions_user[int(record['user'])]:
                actions_user[int(record['user'])][int(record['asset'])] = 0
            actions_user[int(record['user'])][int(record['asset'])] = actions_user[int(record['user'])][int(record['asset'])] + record['duration']
            if int(record['asset']) not in actions:
                actions[int(record['asset'])] = 0
            actions[int(record['asset'])] = actions[int(record['asset'])] + record['duration']

    global search
    query = "SELECT * FROM search WHERE time >= " + str(time.time_ns() - 31536000000000000)
    result = client.query(query)
    for table in result:
        for record in table:
            d = {'location': record['location'], 'info': record['info']}
            if record['user'] not in search:
                search[int(record['user'])] = []
            search[int(record['user'])].append(d)

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

        dic['type'] = 7
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
            dic['price'] = int(asset.asset_info['price'])

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

        assets_all[asset.asset_id] = dic

        if asset.asset_status == 1:
            dic['details'] = []
            dic['title'] = []
            if 'details' in asset.asset_info:
                words = asset.asset_info['details'].lower().split()
                words = re.split('[^a-z]', str(words))
                words = [x for x in words if x != '']
                len_sum = len_sum + len(words)
                doclen[asset] = len(words)
                dic['details'] = wordsanalysis(words)

            words = asset.asset_title.lower().split()
            words = re.split('[^a-z0-9]', str(words))
            words = [x for x in words if x != '']
            dic['title'] = wordsanalysis(words)
            assets_now[asset.asset_id] = dic
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
    global popularity_value
    pop_nom = minmax(popularity)
    act_nom = minmax(actions)
    for asset in assets_now:
        p = 0
        a = 0
        if asset in pop_nom:
            p = (pop_nom[asset] + 0.01) * 3
        if asset in act_nom:
            a = act_nom[asset] + 0.01
        popularity_value[asset] = p + a
    return jsonify({
        "code": 200,
        "msg": "OK",
        "data": popularity_value
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


@application.route('/usermatrix')
def get_user_matrix():
    """
    1. region
    2. type
    3. area
    4. price
    5. room
    6. bathroom
    7. garage
    8. year_built
    9. description
    """
    area_sort = sorted(assets_all.items(), key=lambda x: x[1]['area'])
    price_sort = sorted(assets_all.items(), key=lambda x: x[1]['price'])
    year_sort = sorted(assets_all.items(), key=lambda x: x[1]['year'])
    first = int(len(assets_all) / 5)
    second = int(len(assets_all) / 5) * 2
    third = int(len(assets_all) / 5) * 3
    forth = int(len(assets_all) / 5) * 4
    matrix = {}
    for user in search:
        if user not in matrix:
            matrix[user] = {}
        for record in search[user]:
            if 'location' in record:
                if 'region' in record['location']:
                    if 'region' not in matrix[user]:
                        matrix[user]['region'] = {}
                    if record['location']['region'] not in matrix[user]['region']:
                        matrix[user]['region'][record['location']['region']] = 0
                    matrix[user]['region'][record['location']['region']] = matrix[user]['region'][
                                                                               record['location']['region']] + 100

            if 'info' in record:
                if 'type' in record['info']:
                    if 'type' not in matrix[user]:
                        matrix[user]['type'] = {}
                    if record['info']['type'] not in matrix[user]['type']:
                        matrix[user]['type'][record['info']['type']] = 0
                    matrix[user]['type'][record['info']['type']] = matrix[user]['type'][record['info']['type']] + 100

                if 'area' in record['info']:
                    min = getstate(area_sort, record['info']['area'][0], 'area', first, second, third, forth)
                    max = getstate(area_sort, record['info']['area'][1], 'area', first, second, third, forth)
                    if 'area' not in matrix['user']:
                        matrix[user]['area'] = {}
                    for i in range(min, max):
                        if i not in matrix[user]['area']:
                            matrix[user]['area'][i] = 0
                        matrix[user]['area'][i] = matrix[user]['area'][i] + 100

                if 'price' in record['info']:
                    min = getstate(area_sort, record['info']['area'][0], 'price', first, second, third, forth)
                    max = getstate(area_sort, record['info']['area'][1], 'price', first, second, third, forth)
                    if 'price' not in matrix['user']:
                        matrix[user]['price'] = {}
                    for i in range(min, max):
                        if i not in matrix[user]['price']:
                            matrix[user]['price'][i] = 0
                        matrix[user]['price'][i] = matrix[user]['price'][i] + 100

                if 'room' in record['info']:
                    if 'room' not in matrix[user]:
                        matrix[user]['room'] = {}
                    if record['info']['room'] not in matrix[user]['room']:
                        matrix[user]['room'][record['info']['room']] = 0
                    matrix[user]['room'][record['info']['room']] = matrix[user]['room'][record['info']['room']] + 100

                if 'bathroom' in record['info']:
                    if 'bathroom' not in matrix[user]:
                        matrix[user]['bathroom'] = {}
                    if record['info']['bathroom'] not in matrix[user]['bathroom']:
                        matrix[user]['bathroom'][record['info']['bathroom']] = 0
                    matrix[user]['bathroom'][record['info']['bathroom']] = matrix[user]['bathroom'][
                                                                               record['info']['bathroom']] + 100

                if 'year' in record['info']:
                    min = getstate(area_sort, record['info']['year'][0], 'price', first, second, third, forth)
                    max = getstate(area_sort, record['info']['year'][1], 'price', first, second, third, forth)
                    if 'year' not in matrix['user']:
                        matrix[user]['year'] = {}
                    for i in range(min, max):
                        if i not in matrix[user]['year']:
                            matrix[user]['year'][i] = 0
                        matrix[user]['year'][i] = matrix[user]['year'][i] + 100

                if 'garage' in record['info']:
                    if 'garage' not in matrix[user]:
                        matrix[user]['garage'] = {}
                    if record['info']['garage'] not in matrix[user]['garage']:
                        matrix[user]['garage'][record['info']['garage']] = 0
                    matrix[user]['garage'][record['info']['garage']] = matrix[user]['garage'][
                                                                           record['info']['garage']] + 100

    for user in actions_user:
        if user not in matrix:
            matrix[user] = {}
        for asset in actions_user[user]:
            if 'region' not in matrix[user]:
                matrix[user]['region'] = {}
            if 'region' in assets_all[asset]:
                if assets_all[asset]['region'] not in matrix[user]['region']:
                    matrix[user]['region'][assets_all[asset]['region']] = 0
                matrix[user]['region'][assets_all[asset]['region']] = matrix[user]['region'][assets_all[asset]['region']] + \
                                                                  actions_user[user][asset]

            if 'type' not in matrix[user]:
                matrix[user]['type'] = {}
            if assets_all[asset]['type'] not in matrix[user]['type']:
                matrix[user]['type'][assets_all[asset]['type']] = 0
            matrix[user]['type'][assets_all[asset]['type']] = matrix[user]['type'][assets_all[asset]['type']] + \
                                                              actions_user[user][asset]

            area_state = getstate(area_sort, assets_all[asset]['area'], 'area', first, second, third, forth)
            if 'area' not in matrix[user]:
                matrix[user]['area'] = {}
            if area_state not in matrix[user]['area']:
                matrix[user]['area'][area_state] = 0
            matrix[user]['area'][area_state] = matrix[user]['area'][area_state] + actions_user[user][asset]

            price_state = getstate(price_sort, assets_all[asset]['price'], 'price', first, second, third, forth)
            if 'price' not in matrix[user]:
                matrix[user]['price'] = {}
            if price_state not in matrix[user]['price']:
                matrix[user]['price'][price_state] = 0
            matrix[user]['price'][price_state] = matrix[user]['price'][price_state] + actions_user[user][asset]

            if 'room' not in matrix[user]:
                matrix[user]['room'] = {}
            if assets_all[asset]['room'] not in matrix[user]['room']:
                matrix[user]['room'][assets_all[asset]['room']] = 0
            matrix[user]['room'][assets_all[asset]['room']] = matrix[user]['room'][assets_all[asset]['room']] + \
                                                              actions_user[user][asset]
            if 'bathroom' not in matrix[user]:
                matrix[user]['bathroom'] = {}
            if assets_all[asset]['bathroom'] not in matrix[user]['bathroom']:
                matrix[user]['bathroom'][assets_all[asset]['bathroom']] = 0
            matrix[user]['bathroom'][assets_all[asset]['bathroom']] = matrix[user]['bathroom'][assets_all[asset]['bathroom']] + actions_user[user][asset]

            year_state = getstate(year_sort, assets_all[asset]['year'], 'year', first, second, third, forth)
            if 'year' not in matrix[user]:
                matrix[user]['year'] = {}
            if year_state not in matrix[user]['year']:
                matrix[user]['year'][year_state] = 0
            print(user, year_state, matrix[user]['year'])
            matrix[user]['year'][year_state] = matrix[user]['year'][year_state] + actions_user[user][asset]

            if 'garage' not in matrix[user]:
                matrix[user]['garage'] = {}
            if assets_all[asset]['garage'] not in matrix[user]['garage']:
                matrix[user]['garage'][assets_all[asset]['garage']] = 0
            matrix[user]['garage'][assets_all[asset]['garage']] = matrix[user]['garage'][assets_all[asset]['garage']] + \
                                                                  actions_user[user][asset]

            if 'description' not in matrix[user]:
                matrix[user]['description'] = {}

            for words in assets_all[asset]['details']:
                if words not in matrix[user]['description']:
                    matrix[user]['description'][words] = 0
                matrix[user]['description'][words] = matrix[user]['description'][words] + actions_user[user][asset]

            for words in assets_all[asset]['title']:
                if words not in matrix[user]['description']:
                    matrix[user]['description'][words] = 0
                matrix[user]['description'][words] = matrix[user]['description'][words] + actions_user[user][asset] * 2
        for item in matrix[user]:
            matrix[user][item] = sorted(matrix[user][item], matrix[user][item].get, reverse=True)[0]
    return matrix


def getstate(list, num, attribute, first, second, third, forth):
    if num < list[first][1][attribute]:
        state = 0
    elif num < list[second][1][attribute]:
        state = 1
    elif num < list[third][1][attribute]:
        state = 2
    elif num < list[forth][1][attribute]:
        state = 3
    else:
        state = 4
    return state


@application.route('/retrieval', methods=['GET', 'POST'])
def retrieval():
    """
    location:
    'longitude': float
    'latitude': float
    'region': string
    'subregion': string
    'street': string
    info:
    'type': int
    'area': [min, max]
    'price': [min, max]
    'room': int
    'bathroom': int
    'garage': int
    'year_built' [min, max]
    'description': string
    """
    result = {}
    # get data
    user = None
    location = None
    info = None
    if request.method == "POST":
        user = int(request.form.get('user'))
        location = json.loads(request.form.get('location'))
        info = json.loads(request.form.get('info'))

    # analysis details
    query = []
    if info is not None:
        if len(info['details']) > 0:
            info['details'] = info['details'].strip().lower()
            info['details'] = re.split('[^a-z|0-9]', str(info['details']))
            info['details'] = [x for x in info['details'] if x != '']
            for w in info['details']:
                if w not in stopwords:
                    if w not in cache:
                        cache[w] = p.stem(w)
                    w = cache[w]
                    query.append(w)
    dis_max = 0
    dis_min = 2147483647
    det_max = 0
    for asset in assets_now:
        if location['subregion'] == assets_now[asset]['subregion']:
            result[asset] = {}

            # calculate the z score for distance
            result[asset]['distance'] = sqrt(pow((location['longitude'] - assets_now[asset]['longitude']), 2) +
                                             pow((location['latitude'] - assets_now[asset]['latitude']), 2))

            if result[asset]['distance'] > dis_max:
                dis_max = result[asset]['distance']
            if result[asset]['distance'] < dis_min:
                dis_min = result[asset]['distance']

            # matching degree
            result[asset]['match'] = 0
            if len(location['subregion']) > 0:
                if location['subregion'] == assets_now[asset]['subregion']:
                    result[asset]['match'] = result[asset]['match'] + 1
            if len(location['street']) > 0:
                if location['street'] == assets_now[asset]['street']:
                    result[asset]['match'] = result[asset]['match'] + 1

            if info is not None:
                # matching degree
                if info['type'] == 7 or info['type'] == assets_now[asset]['type']:
                    result[asset]['match'] = result[asset]['match'] + 1

                if info['area'][1] > area_max:
                    info['area'][1] = area_max
                elif info['area'][0] < area_min:
                    info['area'][0] = area_min
                if info['area'][1] > assets_now[asset]['area'] > info['area'][0]:
                    result[asset]['match'] = result[asset]['match'] + 1

                if info['price'][1] > price_max:
                    info['price'][1] = price_max
                elif info['price'][0] < price_min:
                    info['price'][0] = price_min
                if info['price'][1] > assets_now[asset]['price'] > info['price'][0]:
                    result[asset]['match'] = result[asset]['match'] + 1

                if info['year'][1] > year_max:
                    info['year'][1] = year_max
                elif info['year'][0] < year_min:
                    info['year'][0] = year_min
                if info['year'][1] > assets_now[asset]['year'] > info['year'][0]:
                    result[asset]['match'] = result[asset]['match'] + 1

                if assets_now[asset]['room'] > info['room']:
                    result[asset]['match'] = result[asset]['match'] + 1
                if assets_now[asset]['bathroom'] > info['bathroom']:
                    result[asset]['match'] = result[asset]['match'] + 1
                if assets_now[asset]['garage'] > info['garage']:
                    result[asset]['match'] = result[asset]['match'] + 1
                res = 0
                if len(assets_now[asset]['details']) > 0:
                    print(assets_now[asset]['details'])
                    for q in query:
                        if q in assets_now[asset]['details']:
                            res = res + assets_now[asset]['details'][q]
                if len(assets_now[asset]['title']) > 0:
                    print(assets_now[asset]['title'])
                    for q in query:
                        if q in assets_now[asset]['title']:
                            res = res + (assets_now[asset]['title'][q]) * 2
                if res > det_max:
                    det_max = res
                result[asset]['details'] = res
    if user is None:
        print('1111')
        sort = sorted(popularity_value, key=popularity_value.get, reverse=True)
        for i in result:
            result[i]["distance"] = 1 - ((result[i]["distance"] - dis_min) / (dis_max - dis_min))
            result[i]['match'] = result[i]["match"] / 7
            result[i]['details'] = result[i]['details'] / det_max
            result[i]['pop'] = popularity_value[i] / popularity_value[sort[0]]
        print(result)

    return "success"


@application.route('/addaction')
def addaction():
    for i in range(0, 100):
        json_body = [
            {
                "measurement": "browse",
                "tags": {
                    "user": str(random.randint(3, 100)),
                    "asset": str(random.randint(3, 100))
                },
                "fields": {
                    "duration": random.randint(3, 100),
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
    j = 1
    for i in df.index:
        version = 1
        id = j
        user = random.randint(1, 995)
        agent = []
        inspector = []
        type = random.randint(1, 2)
        j = j + 1
        location = {
            'type': "FeatureCollection",
            'features': [
                {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            'coordinates': [df.loc[i, 'longitude'] / 1000000, df.loc[i, 'latitude'] / 1000000],
                        },
                        "properties": {
                            "name": "New York",
                            "description": 'New York',
                            'country': 'America',
                            'region': zip_code[str(int(df.loc[i, 'zip']))]['region'].strip(),
                            'subregion': zip_code[str(int(df.loc[i, 'zip']))]['subregion'].strip(),
                            'street': ''
                        }
                    }
            ]
        }
        info = {'bathroom': int(df.loc[i, 'bathroom']),
                'room': int(df.loc[i, 'bedroom']),
                'area': int(df.loc[i, 'size']),
                'year_built': int(df.loc[i, 'year']),
                'price': int(df.loc[i, 'price']),
                'garage': int(df.loc[i, 'garage']),
                'type': random.randint(1, 6)
                }
        if i < 5000:
            title = 'house'
            if i < 1234:
                title = 'a second hand ' + title
        else:
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
            Asset(revision=version,
                  asset_id=id,
                  asset_user=user,
                  asset_agent=agent,
                  asset_inspector=inspector,
                  asset_title=title,
                  asset_location=location,
                  asset_info=info,
                      asset_open=datetime.datetime.now(),
                      asset_status=1,
                      asset_type=type))
            # print(id, location)
    print(s)
    print(z)
    print(zz)
    session.commit()
    return "success"
