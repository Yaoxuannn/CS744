# coding=utf-8
from nameko.rpc import rpc
from nameko.standalone.rpc import ClusterRpcProxy
from datetime import datetime
from sqlalchemy import Column, String, DateTime
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()
engine = create_engine('sqlite:///../posting.db')
Session = sessionmaker()
Session.configure(bind=engine)

CONFIG = {'AMQP_URI': "amqp://guest:guest@localhost"}


class Posting(Base):
    __tablename__ = "posting"

    posting_id = Column(String, primary_key=True)
    sender = Column(String, nullable=False)
    posting_time = Column(DateTime)
    approve_time = Column(DateTime)
    posting_type = Column(String, default="discussion")
    posting_topic = Column(String)
    message = Column(String, nullable=False)
    group_id = Column(String, nullable=False)
    posting_status = Column(String, default="open")
    discussion_id = Column(String)


class Reply(Base):
    __tablename__ = "reply"

    posting_id = Column(String, primary_key=True)
    discussion_id = Column(String, nullable=False)
    sender = Column(String, nullable=False)
    posting_time = Column(DateTime)
    message = Column(String, nullable=False)


class PostingService(object):
    name = "posting_service"

    @staticmethod
    def generate_posting_id():
        return ''

    @staticmethod
    def generate_discussion_id():
        return ''

    @rpc
    def add_posting(self, sender, posting_type, topic, message, gid):
        session = Session()
        with ClusterRpcProxy(CONFIG) as _rpc:
            user_type = _rpc.user_service.check_user_type(sender)
        new_posting = Posting(
            posting_id=self.generate_posting_id(),
            sender=sender,
            posting_time=datetime.now(),
            posting_type=posting_type,
            posting_topic=topic,
            message=message,
            group_id=gid
        )
        with ClusterRpcProxy(CONFIG) as _rpc:
            if user_type == 'admin' or posting_type == 'dissemination':
                new_posting.posting_status = 'approved'
            else:
                new_posting.discussion_id = self.generate_discussion_id()
                event_id = _rpc.event_service.add_event(event_type="posting", initiator=sender, created_time=new_posting.posting_time)
        session.add(new_posting)
        session.commit()
        session.close()
        if event_id:
            return event_id, new_posting.discussion_id
        if user_type == 'admin' or posting_type == 'dissemination':
            return True

    # @rpc
    # def get_posting_by_group(self):
    #     pass

    @rpc
    def get_dissemination(self, group_id):
        pass

    @rpc
    def get_discussion_by_last_id(self, last_id, limit=1):
        session = Session()
        postings = session.query(Posting)\
            .filter(Posting.posting_type == 'discussion')\
            .filter(Posting.posting_status == 'approved')\
            .order_by(Posting.posting_time.desc())\
            .all()
        session.close()
        index = 0
        data = []
        for posting in postings:
            if posting.last_id == last_id:
                index = postings[posting]
                break
        new_last_id = postings[0].posting_id
        for posting in postings[:index+limit+1]:
            data.append({
                "topic": posting.posting_topic,
                "sender": posting.sender,
                "posting_time": posting.posting_time,
                "approve_time": posting.approve_time,
                "message": posting.message,
                "discussion_id": posting.discussion_id,
            })
        return data, new_last_id

    @rpc
    def get_last_discussion(self):
        session = Session()
        last_one = session.query(Posting)\
            .filter(Posting.posting_type == 'discussion')\
            .filter(Posting.posting_status == 'approved')\
            .order_by(Posting.posting_time.desc())\
            .first()
        session.close()
        return [{
            "topic": last_one.posting_topic,
            "sender": last_one.sender,
            "posting_time": last_one.posting_time,
            "approve_time": last_one.approve_time,
            "message": last_one.message,
            "discussion_id": last_one.discussion_id,
        }], last_one.posting_id

    @rpc
    def reply(self, sender, discussion_id, message):
        session = Session()
        new_reply = Reply(
            posting_id=self.generate_posting_id(),
            discussion_id=discussion_id,
            sender=sender,
            posting_time=datetime.now(),
            message=message
        )
        session.add(new_reply)
        session.commit()
        session.close()
        return new_reply.posting_id

    @rpc
    def get_replies(self, discussion_id, limit=5):
        session = Session()
        replies = session.query(Reply)\
            .filter(Reply.discussion_id == discussion_id)\
            .order_by(Reply.posting_time.asc())\
            .limit(limit=limit)
        data = []
        for r in replies:
            data.append({
                "posting_id": r.posting_id,
                "sender": r.sender,
                "message": r.message,
                "posting_time": r.posting_time
            })
        return data

    @rpc
    def search_posting(self, topic, start_date, end_date, user, limit=1):
        pass
