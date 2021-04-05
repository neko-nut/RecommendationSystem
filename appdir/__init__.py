from flask import Flask
from appdir.config import Config
from flask_sqlalchemy import SQLAlchemy

application = Flask(__name__)
application.config.from_object(Config) # Load Configuration
db = SQLAlchemy(application)


from appdir import routes, models
