# coding=utf-8
from nameko.rpc import rpc
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String


Base = declarative_base()
engine = create_engine('sqlite:///../event.db')
Session = sessionmaker()
Session.configure(bind=engine)


class Event(Base):
    __tablename__ = "events"

    pass
