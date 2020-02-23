# coding=utf-8
import random
import string
from hashlib import sha1
from time import time
from uuid import uuid4

from nameko.rpc import rpc
from nameko.standalone.rpc import ClusterRpcProxy
from sqlalchemy import Column, Text, Text
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

    user_id = Column(Text, primary_key=True, unique=True, nullable=False)
    user_name = Column(Text, unique=True)
    user_fullname = Column(Text)
    user_email = Column(Text)
    user_phone = Column(Text)
    user_status = Column(Text, default=0)
    user_token = Column(Text, unique=True)
    user_type = Column(Text, nullable=False)
    last_read_posting_id = Column(Text)
    login_code = Column(Text)
    addition_info = Column(Text)
    associate_user = Column(Text)
    preferred_info = Column(Text, default="email")


class UserSecret(Base):
    __tablename__ = "user_secret"

    user_id = Column(Text, primary_key=True)
    secret = Column(Text, nullable=False)


class UserService(object):
    name = "user_service"

    sha1 = sha1()

    @rpc
    def check_user_type_by_id(self, user_id):
        session = Session()
        check_user = session.query(User).filter(User.user_id == user_id).first()
        session.close()
        return check_user.user_type if check_user else None

    @rpc
    def check_user_type_by_token(self, token):
        session = Session()
        check_user = session.query(User).filter(User.user_token == token).first()
        session.close()
        return check_user.user_type if check_user else None

    @rpc
    def get_user_list(self, user_type="*"):
        session = Session()
        data = []
        if user_type == "*":
            user_list = session.query(User).all()
        else:
            user_list = session.query(User) \
                .filter(User.user_type == user_type) \
                .filter(User.user_status == "Verified") \
                .all()
        for user in user_list:
            data.append({
                "userID": user.user_id,
                "userName": user.user_name,
                "userType": user.user_type
            })
        return data

    @rpc
    def get_user_info(self, user_id):
        session = Session()
        check_user = session.query(User).filter(User.user_id == user_id).first()
        if not check_user:
            return None
        session.close()
        return {
            "user_id": check_user.user_id,
            "user_name": check_user.user_name,
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

    @staticmethod
    def generate_password():
        return uuid4().hex[:12]

    @staticmethod
    def generate_user_id():
        return uuid4().hex

    @rpc
    def user_register(self, user_info):
        session = Session()
        existed_username = session.query(User.user_name).filter(User.user_name == user_info['username']).first()
        if existed_username:
            return 10002, "Username has already been taken", None
        if user_info['usertype'] in ["patient", "nurse"] and not user_info['associateID']:
            return 10002, "Missing Value", None
        if user_info['usertype'] in ["patient", "nurse"] and self.check_user_type_by_id(
                user_info['associateID']) != "physician":
            return 10002, "Wrong Value", None
        new_user = User(
            user_id=self.generate_user_id(),
            user_fullname=user_info['fullname'],
            user_name=user_info['username'],
            user_type=user_info['usertype'],
            user_email=user_info['email'],
            user_phone=user_info['mobile'],
            user_status="Processing",
            associate_user=user_info['associateID'],
            preferred_info=user_info['preferred'] or "email"
        )
        session.add(new_user)
        session.add(UserSecret(user_id=new_user.user_id, secret=self.generate_password()))
        session.commit()
        with ClusterRpcProxy(CONFIG) as _rpc:
            event_id = _rpc.event_service.add_event(event_type="register", initiator=new_user.user_id,
                                                    target=new_user.user_id)
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
        user_id = session.query(User.user_id).filter(User.user_name == username).first()
        if session.query(UserSecret).filter(UserSecret.user_id == user_id[0], UserSecret.secret == password).first():
            self.sha1.update((username + str(time())).encode())
            token = self.sha1.digest().hex()
            right_user = session.query(User).filter(User.user_name == username).first()
            right_user.user_token = token
            login_code = self.generate_login_code()
            right_user.login_code = login_code
            email_addr = right_user.user_email
            session.commit()
            with ClusterRpcProxy(CONFIG) as _rpc:
                _rpc.mail_service.send_mail(email_addr, "login verification", "Below is your login "
                                                                              "code:<br/><b>%s</b><br/><span>Do not "
                                                                              "share your code!<span>" % login_code)
            return 20000, "OK", token, right_user.user_id
        return 10001, "Wrong credential", None, None

    @rpc
    def change_password(self, token, old_password, new_password):
        session = Session()
        if self.check_user_type_by_token(token) is not None:
            user_id = session.query(User.user_id).filter(User.user_token == token).first()
            user_secret = session.query(UserSecret) \
                .filter(UserSecret.secret == old_password) \
                .filter(UserSecret.user_id == user_id[0]).first()
            if not user_secret:
                return 10001, "User not existed or wrong credential."
            user_secret.secret = new_password
            session.commit()
            session.close()
            return 20000, "OK"
        return 10002, "User not logged in"

    @rpc
    def user_logout(self, token):
        session = Session()
        logged_user = session.query(User).filter(User.user_token == token).first()
        if not logged_user:
            return 10001, "token error/user not logged in"
        logged_user.user_token = None
        logged_user.login_code = None
        session.commit()
        session.close()
        return 20000, "OK"

    @rpc
    def get_last_read_id(self, user_id):
        session = Session()
        posting_id = session.query(User.last_read_posting_id).filter(User.user_id == user_id).first()
        session.close()
        return posting_id

    @rpc
    def set_last_read_id(self, user_id, posting_id):
        session = Session()
        current_user = session.query(User).filter(User.user_id == user_id).first()
        current_user.last_read_posting_id = posting_id
        session.commit()
        session.close()
        return True

    @staticmethod
    def update_user_status(user_id, status):
        session = Session()
        user = session.query(User).filter(User.user_id == user_id).first()
        if not user:
            session.close()
            return False
        user.user_status = status
        session.commit()
        session.close()
        return True

    @rpc
    def verify_user(self, user_id):
        session = Session()
        with ClusterRpcProxy(CONFIG) as _rpc:
            user_email = session.query(User.user_email).filter(User.user_id == user_id).first()
            user_password = session.query(UserSecret.secret).filter(UserSecret.user_id == user_id).first()
            _rpc.mail_service.send_mail(user_email, "Registration approved", "<i>Congratulations!</i><br/>"
                                                                             "The administrator has approved your registration.<br/>"
                                                                             "Here is your password: <b>%s</b>." % user_password)
        return self.update_user_status(user_id, "Verified")

    @rpc
    def reject_user(self, user_id):
        session = Session()
        with ClusterRpcProxy(CONFIG) as _rpc:
            user_email = session.query(User.user_email).filter(User.user_id == user_id).first()
            _rpc.mail_service.send_mail(user_email, "Registration rejected", "<i>Sorry!</i><br/>"
                                                                             "The administrator has rejected your registration.")
        return self.update_user_status(user_id, "Rejected")
