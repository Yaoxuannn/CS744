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
    session = Session()

    # @rpc
    # def create_group(self):
    #     pass

    # @rpc
    # def get_physician(self, group_id):
    #     return self.session.query(Group.physician_id).filter(Group.group_id == group_id).first()
    #
    # @rpc
    # def get_patient(self, group_id):
    #     return self.session.query(Group.patient_id).filter(Group.group_id == group_id).first()
    #
    # @rpc
    # def get_admin(self, group_id):
    #     return self.session.query(Group.administrator_id).filter(Group.group_id == group_id).first()
    #
    # @rpc
    # def get_nurse(self, group_id):
    #     return self.session.query(Group.nurse_id).filter(Group.group_id == group_id).first()
    #
    # @rpc
    # def get_user_groups(self, user_id):
    #     data = []
    #     groups = self.session.query(Group).filter(
    #         or_(Group.physician_id == user_id, Group.nurse_id == user_id, Group.patient_id == user_id,
    #             Group.administrator_id == user_id).all())
    #     for g in groups:
    #         data.append({
    #             "groupID": g.group_id,
    #             "groupName": g.group_name
    #         })
    #     return data
    @rpc
    def add_user_into_group(self, user_id):
        with ClusterRpcProxy(CONFIG) as _rpc:
            user_type = _rpc.user_service.check_user_type_by_id(user_id)
        if user_type in ["nurse", "admin", "physician"]:
            self.session.add(UserGroup(
                user_id=user_id,
                group_id="NPA"
            ))
        if user_type in ["patient", "admin", "physician"]:
            self.session.add(UserGroup(
                user_id=user_id,
                group_id="PPA"
            ))
        self.commit()

    @rpc
    def get_group_by_user_id(self, user_id):
        session = Session()
        groups = session.query(UserGroup.group_id).filter(UserGroup.user_id == user_id).all()
        data = []
        for gid in groups:
            g_name = session.query(Group.group_name).filter(Group.group_id == gid[0]).first()
            data.append({
                "gid": gid[0],
                "groupName": g_name[0]
            })
        return data

    @rpc
    def commit(self):
        self.session.commit()

    @rpc
    def rollback(self):
        self.session.rollback()


