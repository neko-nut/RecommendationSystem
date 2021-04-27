import os
basedir = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'you-will-never-guess'

    SQLALCHEMY_TRACK_MODIFICATIONS = False
    Data = os.path.join(basedir, 'static/housedata.csv')
    stopwords = os.path.join(basedir, 'static/stopwords.txt')
    zip = os.path.join(basedir, 'static/zip.txt')
    zips = os.path.join(basedir, 'static/zips.json')