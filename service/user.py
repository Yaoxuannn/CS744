# coding=utf-8
import random
import string
from hashlib import sha1
from time import time
from uuid import uuid4

from nameko.rpc import rpc
from nameko.standalone.rpc import ClusterRpcProxy
from sqlalchemy import Column, String
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()
engine = create_engine('sqlite:///../user.db')
Session = sessionmaker()
Session.configure(bind=engine)

CONFIG = {'AMQP_URI': "amqp://guest:guest@localhost"}


class User(Base):
    __tablename__ = "users"

    user_name = Column(String, primary_key=True, unique=True, nullable=False)
    user_fullname = Column(String)
    user_email = Column(String)
    user_phone = Column(String)
    user_status = Column(String, default=0)
    user_token = Column(String, unique=True)
    user_type = Column(String, nullable=False)
    login_code = Column(String)
    addition = Column(String)
    preferred_info = Column(String, default="email")


class UserSecret(Base):
    __tablename__ = "user_secret"

    user_name = Column(String, primary_key=True)
    secret = Column(String, nullable=False)


def generate_password():
    return uuid4().hex[:6]


class UserService(object):
    name = "user_service"

    sha1 = sha1()

    @staticmethod
    def check_login_status():
        pass

    @rpc
    def check_user_type(self, token):
        session = Session()
        check_user = session.query(User).filter(User.user_token == token).first()
        session.close()
        return check_user.user_type if check_user else None

    @rpc
    def get_user_info(self, user_name):
        session = Session()
        check_user = session.query(User).filter(User.user_name == user_name).first()
        if not check_user:
            return None
        session.close()
        return {
            "user_name": user_name,
            "full_name": check_user.user_fullname,
            "user_type": check_user.user_type,
            "email": check_user.user_email,
            "mobile": check_user.user_phone
        }

    @rpc
    def validate_login_code(self, login_code, token, ts):
        session = Session()
        right_person = session.query(User).filter(User.user_token == token).first()
        session.close()
        if not right_person:
            return 10001, "Wrong token", 0
        if not right_person.user_token:
            return 10002, "User is not logged in", 0
        if str(right_person.login_code) == login_code:
            return 20000, "OK", 1
        return 10001, "Wrong code", 0

    @staticmethod
    def generate_login_code():
        return "".join([string.digits[random.randint(0, 9)] for x in range(6)])

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
            user_status="Processing",
            preferred_info=user_info['preferred'] or "email"
        )
        session.add(new_user)
        session.add(UserSecret(user_name=user_info['username'], secret=generate_password()))
        session.commit()
        with ClusterRpcProxy(CONFIG) as _rpc:
            event_id = _rpc.event_service.add_event(
                event_type="register",
                target=new_user.user_name,
                initiator="admin"
            )
        return 20000, "OK", event_id

    @rpc
    def user_login(self, username, password):
        session = Session()
        existed_user = session.query(User.user_name).filter(User.user_name == username).first()
        if not existed_user:
            session.close()
            return 10002, "Non-existed user", None, None
        if session.query(User).filter(User.user_name == username).first().user_status != "Verified":
            return 10002, "User is not verified", None, None
        if session.query(UserSecret.user_name).filter(UserSecret.user_name == username, UserSecret.secret == password).first():
            self.sha1.update((username + str(time())).encode())
            token = self.sha1.digest().hex()
            right_user = session.query(User).filter(User.user_name == username).first()
            right_user.user_token = token
            login_code = self.generate_login_code()
            right_user.login_code = login_code
            email_addr = right_user.user_email
            session.commit()
            session.close()
            with ClusterRpcProxy(CONFIG) as _rpc:
                _rpc.mail_service.send_mail(email_addr, "login verification", "Below is your login "
                                                                              "code:<br/><b>%s</b><br/><span>Do not "
                                                                              "share your code!<span>" % login_code)
            return 20000, "OK", token, login_code
        return 10001, "Wrong credential", None, None

    @rpc
    def user_logout(self, token):
        session = Session()
        logged_user = session.query(User).filter(User.user_token == token).first()
        if not logged_user:
            return 10001, "token error/user not logged in"
        logged_user.user_token = None
        session.commit()
        session.close()
        return 20000, "OK"
