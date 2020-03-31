# coding=utf-8
from nameko.rpc import rpc
from nameko.standalone.rpc import ClusterRpcProxy
from sqlalchemy import Column, Text, Integer
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()
engine = create_engine('sqlite:///../user.db')
Session = sessionmaker()
Session.configure(bind=engine)

CONFIG = {'AMQP_URI': "amqp://guest:guest@localhost"}


class Group(Base):
    __tablename__ = "groups"

    group_id = Column(Text, primary_key=True)
    group_name = Column(Text, default="GroupName")
    group_status = Column(Text, default="normal")


class UserGroup(Base):
    __tablename__ = "user_group"

    map_id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Text)
    group_id = Column(Text)


class GroupService(object):
    name = "group_service"
    querySession = Session()

    @rpc
    def add_user_into_group(self, user_id):
        session = Session()
        with ClusterRpcProxy(CONFIG) as _rpc:
            user_type = _rpc.user_service.check_user_type_by_id(user_id)
        if user_type in ["nurse", "admin", "physician"]:
            session.add(UserGroup(
                user_id=user_id,
                group_id="NPA"
            ))
        if user_type in ["patient", "admin", "physician"]:
            session.add(UserGroup(
                user_id=user_id,
                group_id="PPA"
            ))
        session.commit()

    @rpc
    def get_group_by_user_id(self, user_id):
        groups = self.querySession.query(UserGroup.group_id).filter(UserGroup.user_id == user_id).all()
        data = []
        for gid in groups:
            g_name = self.querySession.query(Group.group_name).filter(Group.group_id == gid[0]).first()
            data.append({
                "gid": gid[0],
                "groupName": g_name[0]
            })
        return data
