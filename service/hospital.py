# coding=utf-8
from nameko.rpc import rpc
from sqlalchemy import Column, Text, Date
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import exists

Base = declarative_base()
engine = create_engine('sqlite:///../hospital.db')
Session = sessionmaker()
Session.configure(bind=engine)


class HospitalUser(Base):
    __tablename__ = "users"

    hospital_id = Column(Text, primary_key=True)
    firstname = Column(Text, nullable=False)
    lastname = Column(Text, nullable=False)
    mailing_address = Column(Text)
    emails = Column(Text)
    phones = Column(Text)
    date_of_birth = Column(Date)
    gender = Column(Text)
    department = Column(Text)
    physician_name = Column(Text)
    diagnosis = Column(Text)


class HospitalService(object):
    name = "hospital_service"
    session = Session()

    @rpc
    def is_user_exist(self, user_id):
        return self.session.query(exists().where(HospitalUser.hospital_id == user_id)).scalar()

    @rpc
    def check_user_name(self, user_id, first_name, last_name):
        if not self.is_user_exist(user_id):
            return False
        check_user = self.session.query(HospitalUser).filter(HospitalUser.hospital_id == user_id).first()
        flag = True
        if check_user.firstname != first_name:
            flag = False
        if check_user.lastname != last_name:
            flag = False
        return flag

    @rpc
    def rollback(self):
        self.session.rollback()

    @rpc
    def commit(self):
        self.session.commit()
