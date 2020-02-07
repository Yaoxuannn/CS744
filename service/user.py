# coding=utf-8
import random
import string
from hashlib import sha1
from time import time
from uuid import uuid4

from nameko.rpc import rpc
from sqlalchemy import Column, String
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

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


def generate_password():
    return uuid4().hex


class UserService(object):
    name = "user_service"

    sha1 = sha1()

    @staticmethod
    def check_login_status():
        pass

    @rpc
    def login_code_validate(self, session, login_code, token, ts):
        if token not in session:
            return 10001, "Wrong token", 0
        if session[token] == login_code:
            return 20000, "OK", 1
        return 10001, "Wrong code", 0

    @rpc
    def generate_login_code(self):
        return "".join([string.digits[random.randint(0, 9)] for x in range(4)])

    @rpc
    def user_register(self, user_info):
        session = Session()
        existed_username = session.query(User.user_name).filter(User.user_name == user_info['username']).first()
        if existed_username:
            return 10002, "Username has already been taken", None
        new_user = User(
            user_fullname=user_info['fullname'],
            user_name=user_info['username'],
            user_type=user_info['usertype'],
            user_email=user_info['email'],
            user_phone=user_info['mobile'],
            user_status="processing",
            preferred_info=user_info['preferred'] or "email"
        )
        session.add(new_user)
        session.add(UserSecret(user_name=user_info['username'], secret=generate_password()))
        # TODO: 生成eventID
        event_id = ""
        session.commit()
        return 20000, "OK", event_id

    @rpc
    def user_login(self, username, password):
        session = Session()
        existed_user = session.query(User.user_name).filter(User.user_name == username).first()
        if not existed_user:
            session.close()
            return 10002, "Non-existed user", None
        if session.query(User.user_status).filter(User.user_name == username).first() != "Verified":
            return 10002, "User is not verified", None
        if session.query(UserSecret.user_name).filter(UserSecret.user_name == username, UserSecret.secret == password).first():
            self.sha1.update((username + str(time())).encode())
            token = self.sha1.digest().hex()
            right_user = session.query(User).filter(User.user_name == username).first()
            right_user.user_token = token
            session.commit()
            session.close()
            return 20000, "OK", token
        return 10001, "Wrong credential", None

    @rpc
    def user_logout(self, session, token):
        if token not in session:
            return 10001, "User is not logged in"
        session.pop(token)
        return 20000, "OK"
