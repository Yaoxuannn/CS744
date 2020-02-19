# coding=utf-8
from nameko.rpc import rpc
from nameko.standalone.rpc import ClusterRpcProxy
from sqlalchemy import Column, String
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()
engine = create_engine('sqlite:///../posting.db')
Session = sessionmaker()
Session.configure(bind=engine)


