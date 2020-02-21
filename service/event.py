# coding=utf-8
from nameko.rpc import rpc
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, DateTime
from datetime import datetime
from time import time
from hashlib import sha1


Base = declarative_base()
engine = create_engine('sqlite:///../event.db')
Session = sessionmaker()
Session.configure(bind=engine)


class Event(Base):
    __tablename__ = "events"

    event_id = Column(String, primary_key=True)
    event_type = Column(String, nullable=False)
    target = Column(String)
    initiator = Column(String, default="admin")
    created_time = Column(DateTime)
    operated_time = Column(DateTime)
    event_status = Column(String, default="open")


class EventService(object):

    name = "event_service"

    @staticmethod
    def generate_event_id():
        now = datetime.now().strftime("%Y%m%d")
        sha1_obj = sha1()
        sha1_obj.update(str(time()).encode())
        return "{}{}".format(now, sha1_obj.hexdigest()[:7])

    @rpc
    def add_event(self, event_type, initiator, target=None, created_time=datetime.now()):
        event_id = self.generate_event_id()
        session = Session()
        new_event = Event(
            event_id=event_id, event_type=event_type, target=target, initiator=initiator, created_time=created_time
        )
        session.add(new_event)
        session.commit()
        session.close()
        return event_id

    @rpc
    def get_event(self):
        pass

    @rpc
    def get_all_events(self, event_type):
        session = Session()
        data = []
        for event in session.query(Event).filter(Event.event_type == event_type):
            data.append({
                "event_id": event.event_id,
                "initiator": event.initiator,
                "target": event.target,
                "create_time": event.created_time.strftime("%Y-%m-%d %H:%M:%S"),
                "status": event.event_status
            })
        return data

    @staticmethod
    def operate_event(event_id, status):
        session = Session()
        event = session.query(Event).filter(Event.event_id == event_id).first()
        if not event:
            session.close()
            return False
        if event.event_status != "open":
            session.close()
            return False
        event.event_status = status
        event.operated_time = datetime.now()
        session.commit()
        session.close()
        return True

    @rpc
    def approve(self, event_id):
        return self.operate_event(event_id, "approved")

    @rpc
    def reject(self, event_id):
        return self.operate_event(event_id, "rejected")




