from sqlalchemy import DATETIME, INT, Text, JSON, VARCHAR, BIGINT
from sqlalchemy import Column
from sqlalchemy.ext.declarative import declarative_base

SQLALCHEMY_BINDS = {
    'users':        'mysqldb://localhost/users',
    'appmeta':      'sqlite:////path/to/appmeta.db'
}

Base = declarative_base()


class Asset(Base):
    __tablename__ = 'assets'
    # Information about user
    revision = Column(INT)
    asset_id = Column(INT, primary_key=True)
    asset_user = Column(INT)
    asset_agent = Column(JSON)
    asset_inspector = Column(JSON)
    asset_title = Column(Text)
    asset_location = Column(JSON)
    asset_info = Column(JSON)
    asset_open = Column(DATETIME)
    asset_close = Column(DATETIME)
    asset_status = Column(INT)
    asset_type = Column(INT)


class User(Base):
    __tablename__ = 'users'
    __bind_key__ = 'apxe'
    # Information about user
    revision = Column(INT)
    user_id = Column(BIGINT, primary_key=True)
    user_username = Column(VARCHAR(128))
    user_nickname = Column(VARCHAR(32))
    user_password = Column(VARCHAR(32))
    user_salt = Column(VARCHAR(32))
    user_email = Column(VARCHAR(32))
    user_phone = Column(VARCHAR(32))
    user_bio = Column(VARCHAR(32))
    user_timezone = Column(INT)
    user_freeze = Column(VARCHAR(1))
    user_reg_ip = Column(VARCHAR(128))
    user_reg_datetime = Column(DATETIME)
    user_preference = Column(JSON)
    user_google_token = Column(Text)
    user_avatar = Column(Text)
    user_favorites = Column(JSON)
    user_role = Column(INT)

