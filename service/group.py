# coding=utf-8
from nameko.rpc import rpc
from sqlalchemy import Column, String
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
    physician_id = Column(String, nullable=False)
    nurse_id = Column(String)
    patient_id = Column(String)
    administrator_id = Column(String, nullable=False)
    group_status = Column(String, default="normal")


class GroupService(object):
    name = "group_service"
    session = Session()

    @rpc
    def get_physician(self, group_id):
        return self.session.query(Group.physician_id).filter(Group.group_id == group_id).first()

    @rpc
    def get_patient(self, group_id):
        return self.session.query(Group.patient_id).filter(Group.group_id == group_id).first()

    @rpc
    def get_admin(self, group_id):
        return self.session.query(Group.administrator_id).filter(Group.group_id == group_id).first()

    @rpc
    def get_nurse(self, group_id):
        return self.session.query(Group.nurse_id).filter(Group.group_id == group_id).first()


