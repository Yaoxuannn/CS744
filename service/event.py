# coding=utf-8
from datetime import datetime
from hashlib import sha1
from time import time

from nameko.rpc import rpc
from sqlalchemy import Column, Text, DateTime
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()
engine = create_engine('sqlite:///../event.db')
Session = sessionmaker()
Session.configure(bind=engine)


class Event(Base):
    __tablename__ = "events"

    event_id = Column(Text, primary_key=True)
    event_type = Column(Text, nullable=False)
    target = Column(Text)
    initiator = Column(Text, default="admin")
    created_time = Column(DateTime)
    operated_time = Column(DateTime)
    event_status = Column(Text, default="processing")
    additional_info = Column(Text)


class EventService(object):
    name = "event_service"
    querySession = Session()

    @staticmethod
    def generate_event_id():
        now = datetime.now().strftime("%Y%m%d")
        sha1_obj = sha1()
        sha1_obj.update(str(time()).encode())
        return "{}{}".format(now, sha1_obj.hexdigest()[:7])

    @rpc
    def add_event(self, event_type, initiator, target=None, created_time=datetime.now(), event_status='processing',
                  ts=False,
                  operated=False, additional_info=None):
        session = Session()
        event_id = self.generate_event_id()
        new_event = Event(
            event_id=event_id,
            event_type=event_type,
            target=target,
            initiator=initiator,
            event_status=event_status,
            additional_info=additional_info
        )
        if ts:
            new_event.created_time = datetime.fromtimestamp(created_time)
            if operated:
                new_event.operated_time = new_event.created_time
        else:
            new_event.created_time = created_time
            if operated:
                new_event.operated_time = new_event.created_time
        session.add(new_event)
        session.commit()
        return event_id

    @rpc
    def get_event_info(self, event_id):
        check_event = self.querySession.query(Event).filter(Event.event_id == event_id).first()
        if check_event:
            return {
                "event_id": check_event.event_id,
                "event_type": check_event.event_type,
                "target": check_event.target,
                "initiator": check_event.initiator,
                "created_time": check_event.created_time,
                "operated_time": check_event.operated_time,
                "event_status": check_event.event_status,
                "additional_info": check_event.additional_info
            }
        return None

    @rpc
    def get_event_status(self, event_id):
        event = self.get_event_info(event_id)
        return event['event_status'] if event else None

    @rpc
    def get_all_events(self, event_type, status='processing'):
        data = []
        for event in self.querySession.query(Event).filter(Event.event_type == event_type,
                                                           Event.event_status == status):
            data.append({
                "event_id": event.event_id,
                "initiator": event.initiator,
                "target": event.target,
                "created_time": event.created_time.strftime("%Y-%m-%d %H:%M %p"),
                "status": event.event_status,
                "additional_info": event.additional_info
            })
        return data

    @rpc
    def get_cite_event(self, posting_id):
        cite_event = self.querySession.query(Event) \
            .filter(Event.event_type == 'cite') \
            .filter(Event.target == posting_id) \
            .filter(Event.event_status == 'processing') \
            .first()
        if cite_event:
            return self.get_event_info(cite_event.event_id)
        return None

    @classmethod
    def operate_event(cls, event_id, status):
        session = Session()
        event = session.query(Event).filter(Event.event_id == event_id).first()
        if not event:
            return False
        if event.event_status != "processing":
            return False
        event.event_status = status
        event.operated_time = datetime.now()
        session.commit()
        return True

    @rpc
    def approve(self, event_id):
        return self.operate_event(event_id, "approved")

    @rpc
    def reject(self, event_id):
        return self.operate_event(event_id, "rejected")
