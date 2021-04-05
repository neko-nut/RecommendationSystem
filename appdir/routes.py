import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sshtunnel import SSHTunnelForwarder

from appdir import application
from appdir.config import Config
from appdir.models import Asset
from influxdb import InfluxDBClient


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
global cloent
client = InfluxDBClient('127.0.0.1', str(server1.local_bind_port), 'root', 'root', 'example')
client.create_database('apex')


@application.route('/getaction')
def getaction():
    result = client.query('select * from access_log;')
    for table in result:
        print(table)
        print(table[0])
        for record in table:
            print(record)
    return str(result)


@application.route('/getasset')
def getdata():
    global data
    data = list(
        session.query(Asset).filter(Asset.revision == 1).all()
    )
    return "success"












@application.route('/adddata')
def adddata():
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
    j = 0
    for i in df.index:
        version = 1
        id = j
        j = j + 1
        location = {
            'latitude': str(df.loc[i, 'latitude']),
            'longitude': str(df.loc[i, 'longitude']),
            'city': str(int(df.loc[i, 'city'])),
            'county': str(int(df.loc[i, 'county']))
        }
        info = {
            'bathroom': str(int(df.loc[i, 'bathroom'])),
            'bedroom': str(int(df.loc[i, 'bedroom'])),
            'size': str(df.loc[i, 'size']),
            'year': str(int(df.loc[i, 'year'])),
            'price': str(df.loc[i, 'price'])
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
        info['describe'] = ''
        if df.loc[i, 'garage'] > 0:
            info['describe'] = info['describe'] + 'have a ' + str(df.loc[i, 'garage']) + 'square feet garage '
            title = title + 'with garden'
        if df.loc[i, 'pool'] > 0:
            info['describe'] = info['describe'] + 'have ' + str(df.loc[i, 'pool']) + 'swimming pool '
        if df.loc[i, 'yard'] > 0:
            info['describe'] = info['describe'] + 'have a ' + str(df.loc[i, 'yard']) + 'square feet yard '
        session.add(
            Asset(revision=version, asset_id=id, asset_title=title, asset_location=location, asset_info=info))
    session.commit()
    return "success"
