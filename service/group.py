# coding=utf-8
from nameko.rpc import rpc
from sqlalchemy import Column, String, Integer, or_
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

    group_id = Column(String, primary_key=True)
    group_name = Column(String, default="GroupName")
    group_status = Column(String, default="normal")


class UserGroup(Base):
    __tablename__ = "user_group"

    user_id = Column(String)
    group_id = Column(String)


class GroupService(object):
    name = "group_service"
    # session = Session()

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
    def get_group_by_user_id(self, user_id):
        session = Session()
        groups = session.query(UserGroup.group_id).filter(UserGroup.user_id == user_id).all()
        return groups


