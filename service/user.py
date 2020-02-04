from hashlib import md5
from nameko.rpc import rpc
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String


Base = declarative_base()
engine = create_engine('sqlite:///../user.db')
Session = sessionmaker()
Session.configure(bind=engine)


class User(Base):
    __tablename__ = "users"

    user_name = Column(String, primary_key=True, unique=True, nullable=False)
    user_fullname = Column(String)
    user_email = Column(String)
    user_phone = Column(String)
    user_status = Column(String, default=0)
    user_token = Column(String, unique=True)
    user_type = Column(String, nullable=False)
    addition1 = Column(String)
    addition2 = Column(String)
    preferred_info = Column(String, default="EMAIL")


class UserSecret(Base):
    __tablename__ = "user_secret"

    user_name = Column(String, primary_key=True)
    secret = Column(String, nullable=False)


class UserService(object):
    name = "user_service"

    @staticmethod
    def check_login_status():
        pass

    def login_code_validate(self, ):
        pass

    @rpc
    def user_registry(self, ):
        pass

    @rpc
    def user_login(self, username, password):
        session = Session()
        existed_user = session.query(User.user_name).filter(User.user_name == username).first()
        if not existed_user:
            return 10002, "用户不存在"
        if session.query(UserSecret).filter(UserSecret.user_name == username, UserSecret.secret == password).first():
            return 20000, "OK"
        return 10001, "登录信息错误"

    @rpc
    def user_logout(self, ):
        pass
