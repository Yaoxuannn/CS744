# coding=utf-8
from nameko.rpc import rpc
from nameko.standalone.rpc import ClusterRpcProxy
from datetime import datetime
from sqlalchemy import Column, Text, DateTime
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from hashlib import md5
from time import time

Base = declarative_base()
engine = create_engine('sqlite:///../posting.db')
Session = sessionmaker()
Session.configure(bind=engine)

CONFIG = {'AMQP_URI': "amqp://guest:guest@localhost"}


class Posting(Base):
    __tablename__ = "posting"

    posting_id = Column(Text, primary_key=True)
    event_id = Column(Text)
    sender = Column(Text, nullable=False)
    posting_time = Column(DateTime)
    approve_time = Column(DateTime)
    posting_type = Column(Text, default="discussion")
    posting_topic = Column(Text)
    message = Column(Text, nullable=False)
    group_id = Column(Text, nullable=False)
    discussion_id = Column(Text)


class Reply(Base):
    __tablename__ = "reply"

    posting_id = Column(Text, primary_key=True)
    discussion_id = Column(Text, nullable=False)
    sender = Column(Text, nullable=False)
    posting_time = Column(DateTime)
    message = Column(Text, nullable=False)


class PostingService(object):
    name = "posting_service"

    @staticmethod
    def generate_posting_id():
        now = datetime.now().strftime("%Y%m%d")
        md5_obj = md5()
        md5_obj.update(str(time()).encode())
        return "p" + now + md5_obj.hexdigest()[:7]

    @staticmethod
    def generate_discussion_id():
        now = datetime.now().strftime("%Y%m%d")
        md5_obj = md5()
        md5_obj.update(str(time()).encode())
        return 'd' + now + md5_obj.hexdigest()[:12]

    @rpc
    def add_posting(self, sender_id, posting_type, topic, message, gid):
        session = Session()
        with ClusterRpcProxy(CONFIG) as _rpc:
            user_type = _rpc.user_service.check_user_type_by_id(sender_id)
        new_posting = Posting(
            posting_id=self.generate_posting_id(),
            sender=sender_id,
            posting_time=datetime.now(),
            posting_type=posting_type,
            posting_topic=topic,
            message=message,
            group_id=gid
        )
        with ClusterRpcProxy(CONFIG) as _rpc:
            if posting_type == 'dissemination':
                new_posting.event_id = event_id = None
            else:
                new_posting.discussion_id = self.generate_discussion_id()
                ts = new_posting.posting_time.timestamp()
                if user_type == 'admin':
                    event_id = _rpc.event_service.add_event(event_type="posting", initiator=sender_id,
                                                        created_time=ts, event_status='approved', ts=True)
                else:
                    event_id = _rpc.event_service.add_event(event_type="posting", initiator=sender_id,
                                                        created_time=ts, ts=True)
                new_posting.event_id = event_id
        session.add(new_posting)
        session.commit()
        if posting_type == 'dissemination':
            return True
        if event_id:
            return event_id, new_posting.discussion_id

    # @rpc
    # def get_posting_by_group(self):
    #     pass

    @rpc
    def get_dissemination(self, group_id):
        pass

    @rpc
    def get_discussion_by_last_id(self, last_id, limit=1):
        session = Session()
        postings = session.query(Posting) \
            .filter(Posting.posting_type == 'discussion') \
            .order_by(Posting.posting_time.desc()) \
            .all()
        session.close()
        index = 0
        data = []
        if len(postings) < 1:
            return [], None
        approved_postings = []
        for posting in postings:
            with ClusterRpcProxy(CONFIG) as _rpc:
                posting_status = _rpc.event_service.get_event_status(posting.event_id)
                if posting_status == 'approved':
                    approved_postings.append(posting)
        del postings
        for posting in approved_postings:
            if posting.posting_id == last_id:
                index = approved_postings[posting]
                break
        new_last_id = approved_postings[0].posting_id
        for posting in approved_postings[:index + limit + 1]:
            with ClusterRpcProxy(CONFIG) as _rpc:
                user_info = _rpc.user_service.get_user_info(posting.sender)
            data.append({
                "topic": posting.posting_topic,
                "senderID": posting.sender,
                "senderName": user_info['user_name'],  # 或者是fullname
                "posting_time": posting.posting_time,
                "approved_time": posting.approve_time,
                "message": posting.message,
                "discussion_id": posting.discussion_id,
            })
        return data, new_last_id

    @rpc
    def get_last_discussion(self):
        session = Session()
        postings = session.query(Posting) \
            .filter(Posting.posting_type == 'discussion') \
            .order_by(Posting.posting_time.desc()) \
            .all()
        session.close()
        with ClusterRpcProxy(CONFIG) as _rpc:
            for posting in postings:
                posting_status = _rpc.event_service.get_event_status(posting.event_id)
                if posting_status == 'approved':
                    last_one = posting
                    break
            user_info = _rpc.user_service.get_user_info(last_one.sender)
        return [{
            "topic": last_one.posting_topic,
            "senderID": last_one.sender,
            "senderName": user_info["user_name"],
            "posting_time": last_one.posting_time,
            "approved_time": last_one.approve_time,
            "message": last_one.message,
            "discussion_id": last_one.discussion_id,
        }], last_one.posting_id

    @rpc
    def reply(self, sender_id, discussion_id, message):
        session = Session()
        if not session.query(Posting).filter(Posting.discussion_id == discussion_id).first():
            session.close()
            return None
        new_reply = Reply(
            posting_id=self.generate_posting_id(),
            discussion_id=discussion_id,
            sender=sender_id,
            posting_time=datetime.now(),
            message=message
        )
        session.add(new_reply)
        session.commit()
        return new_reply.posting_id

    @rpc
    def get_replies(self, discussion_id, limit=5):
        session = Session()
        replies = session.query(Reply) \
            .filter(Reply.discussion_id == discussion_id) \
            .order_by(Reply.posting_time.asc()) \
            .limit(limit=limit)
        data = []
        with ClusterRpcProxy(CONFIG) as _rpc:
            for r in replies:
                user_info = _rpc.user_service.get_user_info(r.sender)
                data.append({
                    "postingID": r.posting_id,
                    "senderID": r.sender,
                    "senderName": user_info['user_name'],
                    "message": r.message,
                    "posting_time": r.posting_time
                })
        return data

    @rpc
    def search_posting(self, topic, start_date, end_date, user, limit=1):
        pass

    @rpc
    def approve_posting(self, posting_id):
        session = Session()
        target_posting = session.query(Posting).filter(Posting.posting_id == posting_id).first()
        if target_posting:
            event_id = target_posting.event_id
            with ClusterRpcProxy(CONFIG) as _rpc:
                if _rpc.event_service.approve(event_id):
                    return True
        return False

    @rpc
    def reject_posting(self, posting_id):
        session = Session()
        target_posting = session.query(Posting).filter(Posting.posting_id == posting_id).first()
        if target_posting:
            event_id = target_posting.event_id
            with ClusterRpcProxy(CONFIG) as _rpc:
                if _rpc.event_service.reject(event_id):
                    return True
        return False

    @rpc
    def get_posting_list(self):
        posting_list = []
        with ClusterRpcProxy(CONFIG) as _rpc:
            posting_events = _rpc.event_service.get_all_events("posting")
            session = Session()
            for p_event in posting_events:
                sender_info = _rpc.user_service.get_user_info(p_event['initiator'])
                posting_info = session.query(Posting).filter(Posting.event_id == p_event['event_id']).first()
                posting_list.append({
                    "eventID": p_event['event_id'],
                    "postingID": posting_info.posting_id,
                    "senderID": p_event['initiator'],
                    "senderName": sender_info['user_name'],
                    "senderEmail": sender_info['email'],
                    "sendtime": p_event['created_time'],
                    "topic": posting_info.posting_topic,
                    "message": posting_info.message
                })
            session.close()
            return posting_list
