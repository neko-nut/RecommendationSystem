# syslib
import datetime
import json
import math
import os
import re
import time
from math import sqrt

import joblib
# external lib
from flask import request, jsonify
from influxdb import InfluxDBClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sshtunnel import SSHTunnelForwarder

import appdir.porter as porter
# flask env
from appdir import application
from appdir.config import Config
from appdir.models import Asset, User

# MySQL

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

# InfluxDB
server1 = SSHTunnelForwarder(
    ('101.32.220.109', 22),
    ssh_username="root",
    ssh_password="F0G7oFpfYofirZPTl79H",
    remote_bind_address=('127.0.0.1', 8086)
)
server1.start()
client = InfluxDBClient('127.0.0.1', str(server1.local_bind_port), 'root', 'root', 'apex')

# US region info
with open(Config.stopwords, 'r') as f:
    stopwords = set(f.read().split())

# zip_code = {}
# state = {}
# with open(Config.zip, 'r') as f:
#     line = f.readline()
#     while len(line) > 0:
#         line = re.split("[^a-z|A-Z|0-9| ]", str(line))
#         line = [x for x in line if x != '']
#         line = [x for x in line if x != ' ']
#         state[line[4]] = line[3]
#         line = f.readline()
#     state['PR'] = 'Puerto Rico'
#     state['VI'] = 'Virgin Islands'
#     state['AE'] = 'Armed Forces – Europe'
#     state['AA'] = 'Armed Forces – Americas'
#     state['AP'] = 'Armed Forces – Pacific'
#     state['AS'] = 'American Samoa'
#     print(len(state))
#
# with open(Config.zips, 'r') as load_f:
#     load_dict = json.load(load_f)
#     # print(load_dict)
#     for zip in load_dict:
#         if zip['country'] == "US":
#             zip_code[zip['zip_code']] = {"country": zip['country'].split('[^a-z|A-Z| ]')[0], "subregion": zip['city'].split('[^a-z|A-Z| ]')[0], 'region': state[zip['state']].split('[^a-z|A-Z| ]')[0]}
#     print(zip_code)

# asset: time
actions = {}
# user: {asset: time, ...}
actions_user = {}
# user: [{location, info}, {}]
search = {}
# asset: {info, ...}
assets_all = {}
assets_now = {}
# asset: times
popularity = {}
# user: [asset_id]
popularity_user = {}
# asset: value
popularity_value = {}
# user: [asset_id]
preference_user = {}
preference_agent = {}
user_new = set()
# user: {asset: value, ...}
rec_list = {}
# user: {city: city, type: type, ...}
user_feature = {}
agent_feature = {}
# agent: [asset, ...]
agent_asset = {}
# user/agent/asset: [asset/agent, ...]
recommend_user_asset = {}
recommend_user_agent = {}
recommend_asset_agent = {}
recommend_agent_asset = {}

area_min = 2147483647
area_max = 0
price_min = 2147483647
price_max = 0
year_min = 2147483647
year_max = 0

room_max = 0
bathroom_max = 0
garage_max = 0

# 0:[], 1: [], ...
area_list = {}
price_list = {}
year_list = {}

# Porter
p = porter.PorterStemmer()
cache = {}
n = {}

area_sort = []
price_sort = []
year_sort = []
first = 0
second = 0
third = 0
forth = 0


# init
@application.route('/')
def init():
    getaction()
    print('getaction', datetime.datetime.now())
    get_asset()
    print('getasset', datetime.datetime.now())
    get_user()
    print('getuserinfo', datetime.datetime.now())
    getpopularity()
    print('getpopularity', datetime.datetime.now())
    get_user_matrix()
    print('get_user_matrix', datetime.datetime.now())
    get_user_asset_matrix()
    print('get_user_asset_matrix', datetime.datetime.now())
    get_agent_matrix()
    print('get_agent_matrix', datetime.datetime.now())
    get_asset_agent_matrix()
    print('get_asset_agent_matrix', datetime.datetime.now())
    get_user_agent_matrix()
    print('get_user_agent_matrix', datetime.datetime.now())
    get_agent_asset_matrix()
    print('get_agent_asset_matrix', datetime.datetime.now())
    if os.path.exists(Config.user_asset):
        os.remove(Config.user_asset)
    joblib.dump(recommend_user_asset, Config.user_asset)
    if os.path.exists(Config.user_agent):
        os.remove(Config.user_agent)
    joblib.dump(recommend_user_agent, Config.user_agent)
    if os.path.exists(Config.asset_agent):
        os.remove(Config.asset_agent)
    joblib.dump(recommend_asset_agent, Config.asset_agent)
    if os.path.exists(Config.agent_asset):
        os.remove(Config.agent_asset)
    joblib.dump(recommend_agent_asset, Config.agent_asset)
    return jsonify({
        "code": 200,
        "msg": "OK"
    })


def getaction():
    global actions
    global actions_user
    query = "SELECT * FROM browse WHERE time >= " + str(time.time_ns() - 31536000000000000)
    result = client.query(query)
    for table in result:
        for record in table:
            if int(record['user']) != 1017:
                if int(record['user']) not in actions_user:
                    actions_user[int(record['user'])] = {}
                if int(record['asset']) not in actions_user[int(record['user'])]:
                    actions_user[int(record['user'])][int(record['asset'])] = 0
                actions_user[int(record['user'])][int(record['asset'])] = actions_user[int(record['user'])][
                                                                              int(record['asset'])] + record['duration']
                if int(record['asset']) not in actions:
                    actions[int(record['asset'])] = 0
                actions[int(record['asset'])] = actions[int(record['asset'])] + record['duration']

    global search
    query = "SELECT * FROM search WHERE time >= " + str(time.time_ns() - 31536000000000000)
    result = client.query(query)
    for table in result:
        for record in table:
            d = {'location': json.loads(record['location']), 'info': json.loads(record['info']),
                 'asset_type': int(record['asset_type'])}
            if record['user'] not in search:
                search[int(record['user'])] = []
            search[int(record['user'])].append(d)

    return jsonify({
        "code": 200,
        "msg": "OK"
    })


def get_asset():
    # assets
    global assets_all
    global assets_now
    assets = session.query(Asset.asset_id, Asset.asset_info, Asset.asset_location, Asset.asset_agent, Asset.asset_type, Asset.asset_status, Asset.asset_open, Asset.asset_title).filter().all()
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
    for asset in assets:
        for agent in asset.asset_agent:
            if agent not in agent_asset:
                agent_asset[agent] = []
            agent_asset[agent].append(asset.asset_id)
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
        dic['asset_type'] = asset.asset_type
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

        dic['time'] = asset.asset_open

        dic['details'] = []
        dic['title'] = []
        if 'details' in asset.asset_info:
            words = asset.asset_info['details'].lower().split()
            words = re.split('[^a-z]', str(words))
            words = [x for x in words if x != '']
            len_sum = len_sum + len(words)
            doclen[asset.asset_id] = len(words)
            dic['details'] = wordsanalysis(words)

        words = asset.asset_title.lower().split()
        words = re.split('[^a-z0-9]', str(words))
        words = [x for x in words if x != '']
        dic['title'] = wordsanalysis(words)
        dic['state'] = asset.asset_status
        assets_all[asset.asset_id] = dic

    avg_doclen = len_sum / l
    for asset in assets_all:
        if len(assets_all[asset]['details']) > 0:
            for t in assets_all[asset]['details']:
                fi = assets_all[asset]['details'][t]
                assets_all[asset]['details'][t] = (fi * (1 + k) / fi + k * (
                        (1 - b) + (b * doclen[asset]) / avg_doclen)) * math.log(((l - n[t] + 0.5) / (n[t] + 0.5)),
                                                                                2)
            for t in assets_all[asset]['title']:
                fi = assets_all[asset]['title'][t]
                assets_all[asset]['title'][t] = (fi * (1 + k) / fi + k * (
                        (1 - b) + (b * doclen[asset]) / avg_doclen)) * math.log(((l - n[t] + 0.5) / (n[t] + 0.5)),
                                                                                2)
        if assets_all[asset]['state'] == 2:
            assets_now[asset] = assets_all[asset]
    global area_sort
    global price_sort
    global year_sort
    global first
    global second
    global third
    global forth

    area_sort = sorted(assets_now.items(), key=lambda x: x[1]['area'])
    price_sort = sorted(assets_now.items(), key=lambda x: x[1]['price'])
    year_sort = sorted(assets_now.items(), key=lambda x: x[1]['year'])
    first = int(len(assets_now) / 5)
    second = int(len(assets_now) / 5) * 2
    third = int(len(assets_now) / 5) * 3
    forth = int(len(assets_now) / 5) * 4
    # calculate the range
    global area_list
    global price_list
    global year_list

    area_list = {
        0: [0, area_sort[first][1]["area"]],
        1: [(0 + area_sort[first][1]["area"]) / 2,
            (area_sort[first][1]["area"] + area_sort[second][1]["area"]) / 2],
        2: [area_sort[first][1]["area"], area_sort[second][1]["area"]],
        3: [(area_sort[first][1]["area"] + area_sort[second][1]["area"]) / 2,
            (area_sort[second][1]["area"] + area_sort[third][1]["area"]) / 2],
        4: [area_sort[second][1]["area"], area_sort[third][1]["area"]],
        5: [(area_sort[second][1]["area"] + area_sort[third][1]["area"]) / 2,
            (area_sort[third][1]["area"] + area_sort[forth][1]["area"]) / 2],
        6: [area_sort[third][1]["area"], area_sort[forth][1]["area"]],
        7: [(area_sort[third][1]["area"] + area_sort[forth][1]["area"]) / 2,
            (area_sort[forth][1]["area"] + len(assets_all)) / 2],
        8: [area_sort[third][1]["area"], len(assets_all)]
    }
    price_list = {
        0: [0, price_sort[first][1]["price"]],
        1: [(0 + price_sort[first][1]["price"]) / 2,
            (price_sort[first][1]["price"] + price_sort[second][1]["price"]) / 2],
        2: [price_sort[first][1]["price"], price_sort[second][1]["price"]],
        3: [(price_sort[first][1]["price"] + price_sort[second][1]["price"]) / 2,
            (price_sort[second][1]["price"] + price_sort[third][1]["price"]) / 2],
        4: [price_sort[second][1]["price"], price_sort[third][1]["price"]],
        5: [(price_sort[second][1]["price"] + price_sort[third][1]["price"]) / 2,
            (price_sort[third][1]["price"] + price_sort[forth][1]["price"]) / 2],
        6: [price_sort[third][1]["price"], price_sort[forth][1]["price"]],
        7: [(price_sort[third][1]["price"] + price_sort[forth][1]["price"]) / 2,
            (price_sort[forth][1]["price"] + len(assets_all)) / 2],
        8: [price_sort[third][1]["price"], len(assets_all)]
    }
    year_list = {
        0: [0, year_sort[first][1]["year"]],
        1: [(0 + year_sort[first][1]["year"]) / 2,
            (year_sort[first][1]["year"] + year_sort[second][1]["year"]) / 2],
        2: [year_sort[first][1]["year"], year_sort[second][1]["year"]],
        3: [(year_sort[first][1]["year"] + year_sort[second][1]["year"]) / 2,
            (year_sort[second][1]["year"] + year_sort[third][1]["year"]) / 2],
        4: [year_sort[second][1]["year"], year_sort[third][1]["year"]],
        5: [(year_sort[second][1]["year"] + year_sort[third][1]["year"]) / 2,
            (year_sort[third][1]["year"] + year_sort[forth][1]["year"]) / 2],
        6: [year_sort[third][1]["year"], year_sort[forth][1]["year"]],
        7: [(year_sort[third][1]["year"] + year_sort[forth][1]["year"]) / 2,
            (year_sort[forth][1]["year"] + len(assets_all)) / 2],
        8: [year_sort[third][1]["year"], len(assets_all)]}

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


def get_user():
    global popularity
    global popularity_user
    global user_new
    global preference_user
    global preference_agent
    users = session.query(User.user_id, User.user_role, User.user_favorites, User.user_preference, User.user_reg_datetime).all()
    for user in users:
        if user.user_role == 1:
            preference_agent[user.user_id] = user.user_preference
        for asset in user.user_favorites:
            if asset not in popularity:
                popularity[asset] = 0
            popularity[asset] = popularity[asset] + 1
        popularity_user[user.user_id] = user.user_favorites
        preference_user[user.user_id] = user.user_preference
        if user.user_reg_datetime > datetime.datetime.now() - datetime.timedelta(days=30):
            user_new.add(user.user_id)
    return jsonify({
        "code": 200,
        "msg": "OK"
    })


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


def get_user_matrix():
    """
    1. subregion
    2. type
    3. area
    4. price
    5. room
    6. bathroom
    7. garage
    8. year_built
    9. description
    """
    matrix = {}
    search_worth = 100
    favorite_worth = 1000
    preference_worth = 1000
    global room_max
    global bathroom_max
    global garage_max

    for user in search:
        if user not in matrix:
            matrix[user] = {'time': 0, 'subregion': {}, 'type': {}, 'area': {}, 'price': {}, 'room': {}, 'bathroom': {},
                            'year': {}, 'garage': {}, 'description': {}, 'asset_type': {'buy': 0, 'rent': 0}}
        for record in search[user]:
            matrix[user]['time'] = matrix[user]['time'] + search_worth
            if 'asset_type' in record:
                if record['asset_type'] == 1:
                    matrix[user]['asset_type']['buy'] = matrix[user]['asset_type']['buy'] + search_worth
                else:
                    matrix[user]['asset_type']['rent'] = matrix[user]['asset_type']['rent'] + search_worth

            if 'location' in record:
                if 'subregion' in record['location']:
                    if record['location']['subregion'] not in matrix[user]['subregion']:
                        matrix[user]['subregion'][record['location']['subregion']] = 0
                    matrix[user]['subregion'][record['location']['subregion']] = matrix[user]['subregion'][record['location']['subregion']] + search_worth
            if 'info' in record:
                if 'type' in record['info']:
                    if record['info']['type'] not in matrix[user]['type']:
                        matrix[user]['type'][record['info']['type']] = 0
                    matrix[user]['type'][record['info']['type']] = matrix[user]['type'][record['info']['type']] + search_worth

                if 'area' in record['info']:
                    min = getstate(area_sort, record['info']['area'][0], 'area')
                    max = getstate(area_sort, record['info']['area'][1], 'area')
                    for i in range(min, max):
                        if i not in matrix[user]['area']:
                            matrix[user]['area'][i] = 0
                        matrix[user]['area'][i] = matrix[user]['area'][i] + search_worth

                if 'price' in record['info']:
                    min = getstate(area_sort, record['info']['area'][0], 'price')
                    max = getstate(area_sort, record['info']['area'][1], 'price')
                    for i in range(min, max):
                        if i not in matrix[user]['price']:
                            matrix[user]['price'][i] = 0
                        matrix[user]['price'][i] = matrix[user]['price'][i] + search_worth

                if 'room' in record['info']:
                    if record['info']['room'] not in matrix[user]['room']:
                        matrix[user]['room'][record['info']['room']] = 0
                    matrix[user]['room'][record['info']['room']] = matrix[user]['room'][
                                                                       record['info']['room']] + search_worth

                if 'bathroom' in record['info']:
                    if record['info']['bathroom'] not in matrix[user]['bathroom']:
                        matrix[user]['bathroom'][record['info']['bathroom']] = 0
                    matrix[user]['bathroom'][record['info']['bathroom']] = matrix[user]['bathroom'][record['info'][
                        'bathroom']] + search_worth

                if 'year' in record['info']:
                    min = getstate(area_sort, record['info']['year'][0], 'price')
                    max = getstate(area_sort, record['info']['year'][1], 'price')
                    for i in range(min, max):
                        if i not in matrix[user]['year']:
                            matrix[user]['year'][i] = 0
                        matrix[user]['year'][i] = matrix[user]['year'][i] + search_worth

                if 'garage' in record['info']:
                    if record['info']['garage'] not in matrix[user]['garage']:
                        matrix[user]['garage'][record['info']['garage']] = 0
                    matrix[user]['garage'][record['info']['garage']] = matrix[user]['garage'][
                                                                           record['info']['garage']] + search_worth

    for user in actions_user:
        if user not in matrix:
            matrix[user] = {'time': 0, 'subregion': {}, 'type': {}, 'area': {}, 'price': {}, 'room': {}, 'bathroom': {},
                            'year': {}, 'garage': {}, 'description': {}, 'asset_type': {'buy': 0, 'rent': 0}}
        for asset in actions_user[user]:
            matrix = analysis_asset(matrix, asset, user, actions_user[user][asset])

    for user in matrix:
        for asset in popularity_user[user]:
            matrix = analysis_asset(matrix, asset, user, favorite_worth)
        if user not in user_new:
            matrix = analysis_preference(matrix, preference_user, user, preference_worth)

    for user in user_new:
        if user not in matrix:
            matrix[user] = {'time': 0, 'subregion': {}, 'type': {}, 'area': {}, 'price': {}, 'room': {}, 'bathroom': {},
                            'year': {}, 'garage': {}, 'description': {}, 'asset_type': {'buy': 0, 'rent': 0}}
        matrix = analysis_preference(matrix, preference_user, user, preference_worth)

    global user_feature
    for user in matrix:
        user_feature[user] = {}
        if len(matrix[user]['asset_type']) > 0:
            for asset_type in matrix[user]['asset_type']:
                matrix[user]['asset_type'][asset_type] = matrix[user]['asset_type'][asset_type] / matrix[user]['time']
            asset_type_sort = sorted(matrix[user]['asset_type'], key=matrix[user]['asset_type'].get, reverse=True)
            user_feature[user]["asset_type"] = asset_type_sort[0]

        if len(matrix[user]['subregion']) > 0:
            for city in matrix[user]['subregion']:
                matrix[user]['subregion'][city] = matrix[user]['subregion'][city] / matrix[user]['time']
            city_sort = sorted(matrix[user]['subregion'], key=matrix[user]['subregion'].get, reverse=True)
            user_feature[user]["city_first"] = city_sort[0]
            if len(city_sort) > 1 and matrix[user]['subregion'][city_sort[1]] > 0.1:
                user_feature[user]["city_second"] = city_sort[1]
            else:
                user_feature[user]["city_second"] = None

        if len(matrix[user]['type']) > 0:
            for asset_type in matrix[user]['type']:
                matrix[user]['type'][asset_type] = matrix[user]['type'][asset_type] / matrix[user]['time']
            type_sort = sorted(matrix[user]['type'], key=matrix[user]['type'].get, reverse=True)
            user_feature[user]["type"] = type_sort[0]

        if len(matrix[user]['area']) > 0:
            for area in matrix[user]['area']:
                matrix[user]['area'][area] = matrix[user]['area'][area] / matrix[user]['time']
            area_user_sort = sorted(matrix[user]['area'], key=matrix[user]['area'].get, reverse=True)
            user_feature[user]["area"] = area_user_sort[0]
            if len(area_user_sort) > 1 and matrix[user]['area'][area_user_sort[1]] > matrix[user]['area'][
                area_user_sort[0]] - 0.15:
                user_feature[user]["area"] = (area_user_sort[0] + area_user_sort[0]) / 2

        if len(matrix[user]['price']) > 0:
            for price in matrix[user]['price']:
                matrix[user]['price'][price] = matrix[user]['price'][price] / matrix[user]['time']
            price_user_sort = sorted(matrix[user]['price'], key=matrix[user]['price'].get, reverse=True)
            user_feature[user]["price"] = price_user_sort[0]
            if len(price_user_sort) > 1 and matrix[user]['price'][price_user_sort[1]] > matrix[user]['price'][
                price_user_sort[0]] - 0.15:
                user_feature[user]["price"] = (price_user_sort[0] + price_user_sort[0]) / 2

        if len(matrix[user]['room']) > 0:
            for room in matrix[user]['room']:
                matrix[user]['room'][room] = matrix[user]['room'][room] / matrix[user]['time']
            room_sort = sorted(matrix[user]['room'], key=matrix[user]['room'].get, reverse=True)
            user_feature[user]["room"] = room_sort[0]
            if len(room_sort) > 1 and matrix[user]['room'][room_sort[1]] > matrix[user]['room'][room_sort[0]] - 0.15:
                if room_sort[1] < room_sort[0]:
                    user_feature[user]["room"] = room_sort[1]
            if user_feature[user]["room"] > room_max:
                room_max = user_feature[user]["room"]

        if len(matrix[user]['bathroom']) > 0:
            for bathroom in matrix[user]['bathroom']:
                matrix[user]['bathroom'][bathroom] = matrix[user]['bathroom'][bathroom] / matrix[user]['time']
            bathroom_sort = sorted(matrix[user]['bathroom'], key=matrix[user]['bathroom'].get, reverse=True)
            user_feature[user]["bathroom"] = bathroom_sort[0]
            if len(bathroom_sort) > 1 and matrix[user]['bathroom'][bathroom_sort[1]] > matrix[user]['bathroom'][
                bathroom_sort[0]] - 0.15:
                if bathroom_sort[1] < bathroom_sort[0]:
                    user_feature[user]["bathroom"] = bathroom_sort[1]
                else:
                    user_feature[user]["bathroom"] = bathroom_sort[0]
            if user_feature[user]["bathroom"] > bathroom_max:
                bathroom_max = user_feature[user]["bathroom"]

        if len(matrix[user]['garage']) > 0:
            for garage in matrix[user]['garage']:
                matrix[user]['garage'][garage] = matrix[user]['garage'][garage] / matrix[user]['time']
            garage_sort = sorted(matrix[user]['garage'], key=matrix[user]['garage'].get, reverse=True)
            user_feature[user]["garage"] = garage_sort[0]
            if len(garage_sort) > 1 and matrix[user]['garage'][garage_sort[1]] > matrix[user]['garage'][
                garage_sort[0]] - 0.15:
                if garage_sort[1] < garage_sort[0]:
                    user_feature[user]["garage"] = garage_sort[1]
                else:
                    user_feature[user]["garage"] = garage_sort[0]
            if user_feature[user]["garage"] > garage_max:
                garage_max = user_feature[user]["garage"]

        if len(matrix[user]['year']) > 0:
            for year in matrix[user]['year']:
                matrix[user]['year'][year] = matrix[user]['year'][year] / matrix[user]['time']
            year_user_sort = sorted(matrix[user]['year'], key=matrix[user]['year'].get, reverse=True)
            user_feature[user]["year"] = year_user_sort[0]
            if len(year_user_sort) > 1 and matrix[user]['year'][year_user_sort[1]] > matrix[user]['year'][
                year_user_sort[0]] - 0.15:
                user_feature[user]["year"] = (year_user_sort[0] + year_user_sort[0]) / 2

        user_feature[user]["words"] = []
        if len(matrix[user]['description']) > 0:
            for word in matrix[user]['description']:
                matrix[user]['description'][word] = matrix[user]['description'][word] / matrix[user]['time']
            description_sort = sorted(matrix[user]['description'], key=matrix[user]['description'].get, reverse=True)
            for word in description_sort:
                if matrix[user]['description'][word] > 0.5:
                    user_feature[user]["words"].append(word)
    return jsonify({
        "code": 200,
        "msg": "OK"
    })


def getstate(list, num, attribute):
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


def analysis_preference(matrix, preference, user, time):
    matrix[user]['time'] = matrix[user]['time'] + time
    if 'buy_house' in preference[user]:
        if preference[user]['buy_house']:
            matrix[user]['asset_type']['buy'] = matrix[user]['asset_type']['buy'] + time
        else:
            matrix[user]['asset_type']['rent'] = matrix[user]['asset_type']['rent'] + time

    if 'asset_types' in preference[user]:
        for t in preference[user]['asset_types']:
            if t not in matrix[user]['type']:
                matrix[user]['type'][t] = 0
            matrix[user]['type'][t] = matrix[user]['type'][t] + time

    if 'location' in preference[user]:
        if preference[user]['location'] not in matrix[user]['subregion']:
            matrix[user]['subregion'][preference[user]['location']] = 0
        matrix[user]['subregion'][preference[user]['location']] = matrix[user]['subregion'][preference[user]['location']] + time

    if 'area_range' in preference[user]:
        min = getstate(area_sort, preference[user]['area_range'][0], 'area')
        max = getstate(area_sort, preference[user]['area_range'][1], 'area')
        for i in range(min, max):
            if i not in matrix[user]['area']:
                matrix[user]['area'][i] = 0
            matrix[user]['area'][i] = matrix[user]['area'][i] + time

    if 'price_range' in preference[user]:
        min = getstate(price_sort, preference[user]['price_range'][0], 'price')
        max = getstate(price_sort, preference[user]['price_range'][1], 'price')
        for i in range(min, max):
            if i not in matrix[user]['price']:
                matrix[user]['price'][i] = 0
            matrix[user]['price'][i] = matrix[user]['price'][i] + time

    if 'room_num_range' in preference[user]:
        if preference[user]['room_num_range'][0] not in matrix[user]['room']:
            matrix[user]['room'][preference[user]['room_num_range'][0]] = 0
        matrix[user]['room'][preference[user]['room_num_range'][0]] = matrix[user]['room'][preference[user]['room_num_range'][0]] + time

    if 'bathroom_num_range' in preference[user]:
        if preference[user]['bathroom_num_range'][0] not in matrix[user]['bathroom']:
            matrix[user]['bathroom'][preference[user]['bathroom_num_range'][0]] = 0
        matrix[user]['bathroom'][preference[user]['bathroom_num_range'][0]] = matrix[user]['bathroom'][preference[user][
            'bathroom_num_range'][0]] + time

    if 'built_year_range' in preference[user]:
        min = getstate(year_sort, preference[user]['built_year_range'][0], 'year')
        max = getstate(year_sort, preference[user]['built_year_range'][1], 'year')
        for i in range(min, max):
            if i not in matrix[user]['year']:
                matrix[user]['year'][i] = 0
            matrix[user]['year'][i] = matrix[user]['year'][i] + time

    if 'garage_num_range' in preference[user]:
        if preference[user]['garage_num_range'][0] not in matrix[user]['garage']:
            matrix[user]['garage'][preference[user]['garage_num_range'][0]] = 0
        matrix[user]['garage'][preference[user]['garage_num_range'][0]] = matrix[user]['garage'][preference[user]['garage_num_range'][0]] + time
    return matrix


def analysis_asset(matrix, asset, user, time):
    matrix[user]['time'] = matrix[user]['time'] + time
    if 'asset_type' in assets_all[asset]:
        if assets_all[asset]['asset_type'] == 1:
            matrix[user]['asset_type']['buy'] = matrix[user]['asset_type']['buy'] + time
        else:
            matrix[user]['asset_type']['rent'] = matrix[user]['asset_type']['rent'] + time

    if 'subregion' in assets_all[asset]:
        if assets_all[asset]['subregion'] not in matrix[user]['subregion']:
            matrix[user]['subregion'][assets_all[asset]['subregion']] = 0
        matrix[user]['subregion'][assets_all[asset]['subregion']] = matrix[user]['subregion'][assets_all[asset]['subregion']] + time

    if assets_all[asset]['type'] not in matrix[user]['type']:
        matrix[user]['type'][assets_all[asset]['type']] = 0
    matrix[user]['type'][assets_all[asset]['type']] = matrix[user]['type'][assets_all[asset]['type']] + time

    area_state = getstate(area_sort, assets_all[asset]['area'], 'area')
    if area_state not in matrix[user]['area']:
        matrix[user]['area'][area_state] = 0
    matrix[user]['area'][area_state] = matrix[user]['area'][area_state] + time

    price_state = getstate(price_sort, assets_all[asset]['price'], 'price')
    if price_state not in matrix[user]['price']:
        matrix[user]['price'][price_state] = 0
    matrix[user]['price'][price_state] = matrix[user]['price'][price_state] + time

    if assets_all[asset]['room'] not in matrix[user]['room']:
        matrix[user]['room'][assets_all[asset]['room']] = 0
    matrix[user]['room'][assets_all[asset]['room']] = matrix[user]['room'][assets_all[asset]['room']] + time

    if assets_all[asset]['bathroom'] not in matrix[user]['bathroom']:
        matrix[user]['bathroom'][assets_all[asset]['bathroom']] = 0
    matrix[user]['bathroom'][assets_all[asset]['bathroom']] = matrix[user]['bathroom'][
                                                                  assets_all[asset]['bathroom']] + time

    year_state = getstate(year_sort, assets_all[asset]['year'], 'year')
    if year_state not in matrix[user]['year']:
        matrix[user]['year'][year_state] = 0
    matrix[user]['year'][year_state] = matrix[user]['year'][year_state] + time

    if assets_all[asset]['garage'] not in matrix[user]['garage']:
        matrix[user]['garage'][assets_all[asset]['garage']] = 0
    matrix[user]['garage'][assets_all[asset]['garage']] = matrix[user]['garage'][assets_all[asset]['garage']] + time

    for words in assets_all[asset]['details']:
        if words not in matrix[user]['description']:
            matrix[user]['description'][words] = 0
        matrix[user]['description'][words] = matrix[user]['description'][words] + time

    for words in assets_all[asset]['title']:
        if words not in matrix[user]['description']:
            matrix[user]['description'][words] = 0
        matrix[user]['description'][words] = matrix[user]['description'][words] + time * 2
    return matrix


def get_user_asset_matrix():
    global recommend_user_asset
    user_interest = {}
    similar_user = {}
    # user - asset
    for user in actions_user:
        user_interest[user] = {}
        for asset in actions_user[user]:
            user_interest[user][asset] = actions_user[user][asset]
    for user in popularity_user:
        if user not in user_interest:
            user_interest[user] = {}
        for asset in popularity_user[user]:
            if asset not in user_interest[user]:
                user_interest[user][asset] = 0
            user_interest[user][asset] = user_interest[user][asset] + 1000

    for user in user_feature:
        # user - user
        similar_user[user] = {}
        for user2 in user_feature:
            if user_feature[user2]["city_first"] == user_feature[user]["city_first"] or user_feature[user2]["city_first"] == user_feature[user]["city_second"]:
                similar_user[user][user2] = cos_sim_user(user_feature[user], user_feature[user2])

    for user in user_feature:
        prefer = {}
        rec = {}
        for user2 in similar_user[user]:
            if user2 in user_interest:
                for asset in user_interest[user2]:
                    if user not in actions_user or asset not in actions_user[user]:
                        if user not in popularity_user or asset not in popularity_user[user]:
                            if asset not in prefer:
                                prefer[asset] = 0
                            prefer[asset] = prefer[asset] + (similar_user[user][user2] + 1) * user_interest[user2][
                                asset]
        for asset in prefer:
            if asset in assets_now:
                rec[asset] = prefer[asset]
                if assets_now[asset]['time'] > datetime.datetime.now() - datetime.timedelta(days=30):
                    rec[asset] = rec[asset] * 1.2
        recommend_user_asset[user] = sorted(rec, key=rec.get, reverse=True)

        asset_type = user_feature[user]["asset_type"]
        location = {'subregion': user_feature[user]["city_first"]}
        info = {'type': [user_feature[user]["type"]],
                'area': area_list[user_feature[user]["area"] * 2],
                'price': price_list[user_feature[user]["price"] * 2],
                'room': user_feature[user]["room"],
                'bathroom': user_feature[user]["bathroom"],
                'garage': user_feature[user]["garage"],
                'year_built': year_list[user_feature[user]["year"] * 2]
                # 'description': string
                }
        result = ir(location, info, asset_type)
        for asset in result:
            result[asset] = result[asset] * 1.2
        if user_feature[user]["city_second"] is not None:
            location = {'subregion': user_feature[user]["city_second"]}
            result.update(ir(location, info, asset_type))
        res = sorted(result, key=result.get, reverse=True)
        for id in res:
            if id not in recommend_user_asset[user]:
                if user not in actions_user or id not in actions_user[user]:
                    if user not in popularity_user or id not in popularity_user[user]:
                        recommend_user_asset[user].append(id)


def cos_sim_user(sim1, sim2):
    len_user1 = pow(sim1["area"] / 5, 2) + pow(sim1["price"] / 5, 2) + pow(sim1["year"] / 5, 2)
    len_user2 = pow(sim2["area"] / 5, 2) + pow(sim2["price"] / 5, 2) + pow(sim2["year"] / 5, 2)
    sim_score = (sim1["area"] / 5) * (sim2["area"] / 5) + \
                (sim1["price"] / 5) * (sim2["price"] / 5) + \
                (sim1["year"] / 5) * (sim2["year"] / 5)
    if room_max > 0:
        len_user1 = len_user1 + pow(sim1["room"] / room_max, 2)
        len_user2 = len_user2 + pow(sim2["room"] / room_max, 2)
        sim_score = sim_score + (sim1["room"] / room_max) * (sim2["room"] / room_max)
    if bathroom_max > 0:
        len_user1 = len_user1 + pow(sim1["bathroom"] / bathroom_max, 2)
        len_user2 = len_user2 + pow(sim2["bathroom"] / bathroom_max, 2)
        sim_score = sim_score + (sim1["bathroom"] / bathroom_max) * (sim2["bathroom"] / bathroom_max)

    if garage_max > 0:
        len_user1 = len_user1 + pow(sim1["garage"] / garage_max, 2)
        len_user2 = len_user2 + pow(sim2["garage"] / garage_max, 2)
        sim_score = sim_score + (sim1["garage"] / garage_max) * (sim2["garage"] / garage_max)
    len_user1 = sqrt(len_user1)
    len_user2 = sqrt(len_user2)
    if len_user1 + len_user2 > 0:
        sim_score = sim_score / (len_user1 + len_user2)
    if sim2['type'] == sim1['type']:
        sim_score = sim_score + 0.1
    return sim_score


def get_agent_matrix():
    global agent_feature
    matrix = {}
    for agent in preference_agent:
        if agent not in matrix:
            matrix[agent] = {'time': 0, 'subregion': {}, 'type': {}, 'area': {}, 'price': {}, 'room': {},
                             'bathroom': {}, 'year': {}, 'garage': {}, 'description': {},
                             'asset_type': {'buy': 0, 'rent': 0}}
        matrix = analysis_preference(matrix, preference_agent, agent, 1000)
    for agent in agent_asset:
        if agent not in matrix:
            matrix[agent] = {'time': 0, 'subregion': {}, 'type': {}, 'area': {}, 'price': {}, 'room': {},
                             'bathroom': {}, 'year': {}, 'garage': {}, 'description': {},
                             'asset_type': {'buy': 0, 'rent': 0}}
        for asset in agent_asset[agent]:
            matrix = analysis_asset(matrix, asset, agent, 100)

    for agent in matrix:
        agent_feature[agent] = {}
        if len(matrix[agent]['asset_type']) > 0:
            for asset_type in matrix[agent]['asset_type']:
                matrix[agent]['asset_type'][asset_type] = matrix[agent]['asset_type'][asset_type] / matrix[agent][
                    'time']
            asset_type_sort = sorted(matrix[agent]['asset_type'], key=matrix[agent]['asset_type'].get, reverse=True)
            agent_feature[agent]["asset_type"] = asset_type_sort[0]
        if len(matrix[agent]['subregion']) > 0:
            for city in matrix[agent]['subregion']:
                matrix[agent]['subregion'][city] = matrix[agent]['subregion'][city] / matrix[agent]['time']
            city_sort = sorted(matrix[agent]['subregion'], key=matrix[agent]['subregion'].get, reverse=True)
            agent_feature[agent]["city_first"] = city_sort[0]
            if len(city_sort) > 1 and matrix[agent]['subregion'][city_sort[1]] > 0.1:
                agent_feature[agent]["city_second"] = city_sort[1]
            else:
                agent_feature[agent]["city_second"] = None

        if len(matrix[agent]['type']) > 0:
            for asset_type in matrix[agent]['type']:
                matrix[agent]['type'][asset_type] = matrix[agent]['type'][asset_type] / matrix[agent]['time']
            type_sort = sorted(matrix[agent]['type'], key=matrix[agent]['type'].get, reverse=True)
            agent_feature[agent]["type"] = type_sort[0]

        if len(matrix[agent]['area']) > 0:
            for area in matrix[agent]['area']:
                matrix[agent]['area'][area] = matrix[agent]['area'][area] / matrix[agent]['time']
            area_user_sort = sorted(matrix[agent]['area'], key=matrix[agent]['area'].get, reverse=True)
            agent_feature[agent]["area"] = area_user_sort[0]
            if len(area_user_sort) > 1 and matrix[agent]['area'][area_user_sort[1]] > matrix[agent]['area'][
                area_user_sort[0]] - 0.15:
                agent_feature[agent]["area"] = (area_user_sort[0] + area_user_sort[0]) / 2

        if len(matrix[agent]['price']) > 0:
            for price in matrix[agent]['price']:
                matrix[agent]['price'][price] = matrix[agent]['price'][price] / matrix[agent]['time']
            price_user_sort = sorted(matrix[agent]['price'], key=matrix[agent]['price'].get, reverse=True)
            agent_feature[agent]["price"] = price_user_sort[0]
            if len(price_user_sort) > 1 and matrix[agent]['price'][price_user_sort[1]] > matrix[agent]['price'][
                price_user_sort[0]] - 0.15:
                agent_feature[agent]["price"] = (price_user_sort[0] + price_user_sort[0]) / 2

        if len(matrix[agent]['room']) > 0:
            for room in matrix[agent]['room']:
                matrix[agent]['room'][room] = matrix[agent]['room'][room] / matrix[agent]['time']
            room_sort = sorted(matrix[agent]['room'], key=matrix[agent]['room'].get, reverse=True)
            agent_feature[agent]["room"] = room_sort[0]
            if len(room_sort) > 1 and matrix[agent]['room'][room_sort[1]] > matrix[agent]['room'][room_sort[0]] - 0.15:
                if room_sort[1] < room_sort[0]:
                    agent_feature[agent]["room"] = room_sort[1]

        if len(matrix[agent]['bathroom']) > 0:
            for bathroom in matrix[agent]['bathroom']:
                matrix[agent]['bathroom'][bathroom] = matrix[agent]['bathroom'][bathroom] / matrix[agent]['time']
            bathroom_sort = sorted(matrix[agent]['bathroom'], key=matrix[agent]['bathroom'].get, reverse=True)
            agent_feature[agent]["bathroom"] = bathroom_sort[0]
            if len(bathroom_sort) > 1 and matrix[agent]['bathroom'][bathroom_sort[1]] > matrix[agent]['bathroom'][
                bathroom_sort[0]] - 0.15:
                if bathroom_sort[1] < bathroom_sort[0]:
                    agent_feature[agent]["bathroom"] = bathroom_sort[1]
                else:
                    agent_feature[agent]["bathroom"] = bathroom_sort[0]

        if len(matrix[agent]['garage']) > 0:
            for garage in matrix[agent]['garage']:
                matrix[agent]['garage'][garage] = matrix[agent]['garage'][garage] / matrix[agent]['time']
            garage_sort = sorted(matrix[agent]['garage'], key=matrix[agent]['garage'].get, reverse=True)
            agent_feature[agent]["garage"] = garage_sort[0]
            if len(garage_sort) > 1 and matrix[agent]['garage'][garage_sort[1]] > matrix[agent]['garage'][
                garage_sort[0]] - 0.15:
                if garage_sort[1] < garage_sort[0]:
                    agent_feature[agent]["garage"] = garage_sort[1]
                else:
                    agent_feature[agent]["garage"] = garage_sort[0]

        if len(matrix[agent]['year']) > 0:
            for year in matrix[agent]['year']:
                matrix[agent]['year'][year] = matrix[agent]['year'][year] / matrix[agent]['time']
            year_user_sort = sorted(matrix[agent]['year'], key=matrix[agent]['year'].get, reverse=True)
            agent_feature[agent]["year"] = year_user_sort[0]
            if len(year_user_sort) > 1 and matrix[agent]['year'][year_user_sort[1]] > matrix[agent]['year'][
                year_user_sort[0]] - 0.15:
                agent_feature[agent]["year"] = (year_user_sort[0] + year_user_sort[0]) / 2

    return matrix


def get_user_agent_matrix():
    for user in user_feature:
        rec = {}
        for agent in agent_feature:
            rec[agent] = cos_sim_user(user_feature[user], agent_feature[agent])
        recommend_user_agent[user] = sorted(rec, key=rec.get, reverse=True)


def get_agent_asset_matrix():
    for agent in agent_feature:
        asset_type = agent_feature[agent]['asset_type']
        location = {'subregion': agent_feature[agent]["city_first"]}
        info = {'type': [agent_feature[agent]["type"]],
                'area': area_list[agent_feature[agent]["area"] * 2],
                'price': price_list[agent_feature[agent]["price"] * 2],
                'room': agent_feature[agent]["room"],
                'bathroom': agent_feature[agent]["bathroom"],
                'garage': agent_feature[agent]["garage"],
                'year_built': year_list[agent_feature[agent]["year"] * 2]
                # 'description': string
                }
        result = ir(location, info, asset_type)
        for asset in result:
            result[asset] = result[asset] * 1.2
        if agent_feature[agent]["city_second"] is not None:
            location = {'subregion': agent_feature[agent]["city_second"]}
            result.update(ir(location, info, asset_type))
        recommend_agent_asset[agent] = sorted(result, key=result.get, reverse=True)


def get_asset_agent_matrix():
    for asset in assets_now:
        result = {}
        for agent in agent_feature:
            if (agent in agent_asset and asset not in agent_asset[agent]) and (
                    agent_feature[agent]['city_first'] == assets_now[asset]['subregion'] or agent_feature[agent][
                'city_second'] == assets_now[asset]['subregion']):
                result[agent] = 0
                if assets_now[asset]['type'] == 7 or agent_feature[agent]['type'] == assets_now[asset]['type']:
                    result[agent] = result[agent] + 1

                if area_list[agent_feature[agent]['area'] * 2][1] > assets_now[asset]['area'] > \
                        area_list[agent_feature[agent]['area'] * 2][0]:
                    result[agent] = result[agent] + 1

                if price_list[agent_feature[agent]['price']][1] > assets_now[asset]['price'] > \
                        price_list[agent_feature[agent]['price']][0]:
                    result[agent] = result[agent] + 1

                if assets_now[asset]['room'] >= agent_feature[agent]['room']:
                    result[agent] = result[agent] + 1
                if assets_now[asset]['bathroom'] >= agent_feature[agent]['bathroom']:
                    result[agent] = result[agent] + 1
                if assets_now[asset]['garage'] >= agent_feature[agent]['garage']:
                    result[agent] = result[agent] + 1

                if year_list[agent_feature[agent]['year']][1] > assets_now[asset]['year'] > \
                        price_list[agent_feature[agent]['year']][0]:
                    result[agent] = result[agent] + 1
        recommend_asset_agent[asset] = sorted(result, key=result.get)


@application.route('/recommend', methods=['GET', 'POST'])
def recommend():
    user = 0
    length = 20
    if request.method == "POST":
        user = int(request.form.get('user'))
        if length == "":
            length = 20
        else:
            length = int(length)
    matrix = joblib.load(Config.user_asset)
    if len(matrix[user]) < length:
        length = len(matrix[user])
    return jsonify({
        "code": 200,
        "msg": "OK",
        "data": matrix[user][0:length]
    })


@application.route('/getrelevantassets', methods=['GET', 'POST'])
def get_asset_asset():
    asset = 0
    length = 20
    if request.method == "POST":
        asset = int(request.form.get('asset'))
        if length == "":
            length = 20
        else:
            length = int(length)
    location = {
        'longitude': assets_all[asset]['longitude'],
        'latitude': assets_all[asset]['latitude'],
        'region': assets_all[asset]['region'],
        'subregion': assets_all[asset]['subregion'],
        'street': assets_all[asset]['street']
    }
    area_range = [2147483647, 0]
    for range in area_list:
        if area_list[range][1] > assets_all[asset]['area'] > area_list[range][0]:
            if area_range[0] > area_list[range][0]:
                area_range[0] = area_list[range][0]
            if area_range[1] < area_list[range][1]:
                area_range[1] = area_list[range][1]

    price_range = [2147483647, 0]
    for range in price_list:
        if price_list[range][1] > assets_all[asset]['price'] > price_list[range][0]:
            if price_range[0] > price_list[range][0]:
                price_range[0] = price_list[range][0]
            if price_range[1] < price_list[range][1]:
                price_range[1] = price_list[range][1]

    year_range = [2147483647, 0]
    for range in year_list:
        if year_list[range][1] > assets_all[asset]['area'] > year_list[range][0]:
            if year_range[0] > year_list[range][0]:
                year_range[0] = year_list[range][0]
            if year_range[1] < year_list[range][1]:
                year_range[1] = year_list[range][1]

    info = {
        'type': [assets_all[asset]['type']],
        'area': area_range,
        'price': price_range,
        'room': assets_all[asset]['room'],
        'bathroom': assets_all[asset]['bathroom'],
        'garage': assets_all[asset]['garage'],
        'year_built': year_range,
        'description': assets_all[asset]['details']
    }
    result = ir(location, info, assets_all[asset]['asset_type'])
    if len(result) < length:
        length = len(result)
    return jsonify({
        "code": 200,
        "msg": "OK",
        "data": sorted(result, key=result.get, reverse=True)[0:length]
    })


@application.route('/recommendagenttouser', methods=['GET', 'POST'])
def recommend_agent_to_user():
    user = 0
    length = 20
    if request.method == "POST":
        user = int(request.form.get('user'))
        if length == "":
            length = 20
        else:
            length = int(length)
    matrix = joblib.load(Config.user_agent)
    if len(matrix[user]) < length:
        length = len(matrix[user])
    return jsonify({
        "code": 200,
        "msg": "OK",
        "data": matrix[user][0:length]
    })

@application.route('/recommendassettoagent', methods=['GET', 'POST'])
def recommend_asset_to_agent():
    agent = 0
    length = 20
    if request.method == "POST":
        agent = int(request.form.get('agent'))
        if length == "":
            length = 20
        else:
            length = int(length)
    matrix = joblib.load(Config.asset_agent)
    if len(matrix[agent]) < length:
        length = len(matrix[agent])
    return jsonify({
        "code": 200,
        "msg": "OK",
        "data": matrix[agent][0:length]
    })


@application.route('/recommendagenttoasset', methods=['GET', 'POST'])
def recommend_agent_to_asset():
    asset = 0
    length = 20
    if request.method == "POST":
        asset = int(request.form.get('asset'))
        if length == "":
            length = 20
        else:
            length = int(length)
    matrix = joblib.load(Config.agent_asset)
    if len(matrix[asset]) < length:
        length = len(matrix[asset])
    return jsonify({
        "code": 200,
        "msg": "OK",
        "data": matrix[asset][0:length]
    })


@application.route('/retrieval', methods=['GET', 'POST'])
def retrieval():
    # get data
    location = []
    info = []
    asset_type = 1
    length = 20
    if request.method == "POST":
        location = json.loads(request.form.get('location'))
        info = json.loads(request.form.get('info'))
        asset_type = int(request.form.get('asset_type'))
        length = request.form.get('length')
        if length == "":
            length = 20
        else:
            length = int(length)
    result = ir(location, info, asset_type)
    if len(result) < length:
        length = len(result)
    res = sorted(result, key=result.get, reverse=True)[0:length]
    for i in res:
        print(assets_now[i])
    return jsonify({
        "code": 200,
        "msg": "OK",
        "data": sorted(result, key=result.get, reverse=True)[0:length]
    })


def ir(location, info, asset_type):
    '''
    {
        'longitude': float
        'latitude': float
        'region': string
        'subregion': string
        'street': string
    }
    {
        'type': []
        'area': [min, max]
        'price': [min, max]
        'room': int
        'bathroom': int
        'garage': int
        'year_built'[min, max]
        'description': string
    }
    '''
    result = {}
    query = []
    if info is not None:
        if 'details' in info and len(info['details']) > 0:
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
    time_max = 0
    time_min = 2147483647
    for asset in assets_now:
        if location['subregion'] == assets_now[asset]['subregion'] and assets_now[asset]['asset_type'] == asset_type:
            result[asset] = {'distance': 0.0, 'match': 0, 'details': 0, 'time': 0, 'pop': 0}
            if 'longitude' in location and 'latitude' in location:
                result[asset]['distance'] = sqrt(pow((location['longitude'] - assets_now[asset]['longitude']), 2) +
                                                 pow((location['latitude'] - assets_now[asset]['latitude']), 2))

                if result[asset]['distance'] > dis_max:
                    dis_max = result[asset]['distance']
                if result[asset]['distance'] < dis_min:
                    dis_min = result[asset]['distance']

            # matching degree
            if 'subregion' in location:
                if location['subregion'] == assets_now[asset]['subregion']:
                    result[asset]['match'] = result[asset]['match'] + 1
            if 'street' in location:
                if location['street'] == assets_now[asset]['street']:
                    result[asset]['match'] = result[asset]['match'] + 1

            if info is not None:
                # matching degree
                if 'type' in info and assets_now[asset]['type'] in info['type']:
                    result[asset]['match'] = result[asset]['match'] + 1

                if 'area' in info:
                    if info['area'][1] > area_max:
                        info['area'][1] = area_max
                    elif info['area'][0] < area_min:
                        info['area'][0] = area_min
                    if info['area'][1] > assets_now[asset]['area'] > info['area'][0]:
                        result[asset]['match'] = result[asset]['match'] + 2

                if 'price' in info:
                    if info['price'][1] > price_max:
                        info['price'][1] = price_max
                    elif info['price'][0] < price_min:
                        info['price'][0] = price_min
                    if info['price'][1] > assets_now[asset]['price'] > info['price'][0]:
                        result[asset]['match'] = result[asset]['match'] + 2

                if 'year' in info:
                    if info['year'][1] > year_max:
                        info['year'][1] = year_max
                    elif info['year'][0] < year_min:
                        info['year'][0] = year_min
                    if info['year'][1] > assets_now[asset]['year'] > info['year'][0]:
                        result[asset]['match'] = result[asset]['match'] + 1

                if 'room' in info and assets_now[asset]['room'] > info['room']:
                    result[asset]['match'] = result[asset]['match'] + 1
                if 'bathroom' in info and assets_now[asset]['bathroom'] > info['bathroom']:
                    result[asset]['match'] = result[asset]['match'] + 1
                if 'garage' in info and assets_now[asset]['garage'] > info['garage']:
                    result[asset]['match'] = result[asset]['match'] + 1
                res = 0
                if 'details' in info:
                    if len(assets_now[asset]['details']) > 0:
                        for q in query:
                            if q in assets_now[asset]['details']:
                                res = res + assets_now[asset]['details'][q]
                    if len(assets_now[asset]['title']) > 0:
                        for q in query:
                            if q in assets_now[asset]['title']:
                                res = res + (assets_now[asset]['title'][q]) * 2
                    if res > det_max:
                        det_max = res
                    result[asset]['details'] = res

            result[asset]['time'] = (datetime.datetime.now() - assets_now[asset]['time']).total_seconds()
            if result[asset]['time'] > time_max:
                time_max = result[asset]['time']
            if result[asset]['time'] < time_min:
                time_min = result[asset]['time']

    sort = sorted(popularity_value, key=popularity_value.get, reverse=True)
    for i in result:
        if dis_max > dis_min:
            result[i]["distance"] = 1 - ((result[i]["distance"] - dis_min) / (dis_max - dis_min))
        else:
            result[i]["distance"] = 0
        result[i]['match'] = result[i]["match"] / 7
        if det_max > 0:
            result[i]['details'] = result[i]['details'] / det_max
        else:
            result[i]['details'] = 0
        if time_max > time_min:
            result[i]['time'] = 1 - ((result[i]["time"] - time_min) / (time_max - time_min))
        else:
            result[i]['time'] = 0
        if popularity_value[sort[0]] > 0:
            result[i]['pop'] = popularity_value[i] / popularity_value[sort[0]]
        else:
            result[i]['pop'] = popularity_value[i] = 0
        result[i] = result[i]["distance"] + result[i]['match'] + result[i]['details'] + result[i]['time'] + result[i][
            'pop']
    return result


'''
# fill DB
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
        "zip",
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
    s = set()
    z = set()
    zz = set()
    for i in df.index:
        zz.add(df.loc[i, 'zip'])
        if str(int(df.loc[i, 'zip'])) in zip_code:
            s.add(zip_code[str(int(df.loc[i, 'zip']))]['subregion'])
            z.add(df.loc[i, 'zip'])
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
'''

with application.app_context():
    init()
