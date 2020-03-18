# coding=utf-8
from datetime import datetime
from hashlib import md5
from time import time

from nameko.rpc import rpc
from nameko.standalone.rpc import ClusterRpcProxy
from sqlalchemy import Column, Text, DateTime
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

    posting_id = Column(Text, primary_key=True)
    event_id = Column(Text)
    sender = Column(Text, nullable=False)
    posting_time = Column(DateTime)
    posting_type = Column(Text, default="discussion")
    posting_topic = Column(Text)
    message = Column(Text, nullable=False)
    group_id = Column(Text, nullable=False)
    discussion_id = Column(Text)
    posting_status = Column(Text)


class Reply(Base):
    __tablename__ = "reply"

    posting_id = Column(Text, primary_key=True)
    discussion_id = Column(Text, nullable=False)
    cite_event = Column(Text)
    sender = Column(Text, nullable=False)
    posting_time = Column(DateTime)
    message = Column(Text, nullable=False)


class PostingService(object):
    name = "posting_service"
    session = Session()

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
        with ClusterRpcProxy(CONFIG) as _rpc:
            user_type = _rpc.user_service.check_user_type_by_id(sender_id)
            if _rpc.keyword_service.check_discussion_posting(message, topic) is False:
                return False
            new_posting = Posting(
                posting_id=self.generate_posting_id(),
                sender=sender_id,
                posting_time=datetime.now(),
                posting_type=posting_type,
                posting_topic=topic,
                message=message,
                group_id=gid
            )
            if posting_type == 'dissemination':
                new_posting.event_id = event_id = None
            else:
                new_posting.discussion_id = self.generate_discussion_id()
                ts = new_posting.posting_time.timestamp()
                if user_type == 'admin':
                    event_id = _rpc.event_service.add_event(event_type="posting", initiator=sender_id,
                                                            target=new_posting.posting_id,
                                                            created_time=ts, event_status='approved', ts=True,
                                                            operated=True)
                    new_posting.posting_status = "open"
                else:
                    event_id = _rpc.event_service.add_event(event_type="posting", initiator=sender_id,
                                                            created_time=ts, ts=True)
                    new_posting.posting_status = "processing"
                new_posting.event_id = event_id
                _rpc.event_service.commit()
        self.session.add(new_posting)
        self.commit()
        if posting_type == 'dissemination':
            return True
        return event_id, new_posting.discussion_id, new_posting.posting_time

    @rpc
    def get_dissemination(self, group_id):
        postings = self.session.query(Posting) \
            .filter(Posting.posting_type == 'dissemination') \
            .filter(Posting.group_id == group_id) \
            .order_by(Posting.posting_time.desc()) \
            .all()
        data = []
        with ClusterRpcProxy(CONFIG) as _rpc:
            for posting in postings:
                user_info = _rpc.user_service.get_user_info(posting.sender)
                data.append({
                    "topic": posting.posting_topic,
                    "senderID": posting.sender,
                    "senderName": user_info['user_name'],
                    "posting_time": posting.posting_time.strftime("%m/%d/%Y %H:%M %p"),
                    "message": posting.message
                })
        return data

    @staticmethod
    def make_posting_info(postings):
        data = []
        with ClusterRpcProxy(CONFIG) as _rpc:
            for posting in postings:
                user_info = _rpc.user_service.get_user_info(posting.sender)
                data.append({
                    "postingID": posting.posting_id,
                    "postingType": posting.posting_type,
                    "postingStatus": posting.posting_status,
                    "topic": posting.posting_topic,
                    "senderID": posting.sender,
                    "senderName": user_info["user_name"],
                    "posting_time": posting.posting_time.strftime("%m/%d/%Y %H:%M %p"),
                    "message": posting.message,
                    "discussion_id": posting.discussion_id,
                })
        return data

    @staticmethod
    def make_reply_info(replies):
        data = []
        with ClusterRpcProxy(CONFIG) as _rpc:
            for r in replies:
                user_info = _rpc.user_service.get_user_info(r.sender)
                data.append({
                    "postingID": r.posting_id,
                    "senderID": r.sender,
                    "senderName": user_info['user_name'],
                    "message": r.message,
                    "posting_time": r.posting_time.strftime("%m/%d/%Y %H:%M %p")
                })
        return data

    @rpc
    def get_discussions(self, group_id):
        postings = self.session.query(Posting) \
            .filter(Posting.posting_type == 'discussion') \
            .filter(Posting.group_id == group_id) \
            .filter(Posting.posting_status == "open") \
            .order_by(Posting.posting_time.desc()) \
            .all()
        data = self.make_posting_info(postings)
        return data

    @rpc
    def reply(self, sender_id, discussion_id, message):
        discussion_posting = self.session.query(Posting).filter(Posting.discussion_id == discussion_id).first()
        if discussion_posting is None:
            return None
        if discussion_posting.posting_status == "terminated":
            return None
        with ClusterRpcProxy(CONFIG) as _rpc:
            if _rpc.keyword_service.check_discussion_posting(message) is False:
                return False
        new_reply = Reply(
            posting_id=self.generate_posting_id(),
            discussion_id=discussion_id,
            sender=sender_id,
            posting_time=datetime.now(),
            message=message
        )
        self.session.add(new_reply)
        self.commit()
        return new_reply.posting_id, new_reply.posting_time.strftime("%m/%d/%Y %H:%M %p")

    @rpc
    def get_replies(self, discussion_id, limit=8, offset=0):
        replies = self.session.query(Reply) \
            .filter(Reply.discussion_id == discussion_id) \
            .order_by(Reply.posting_time.asc()) \
            .limit(limit) \
            .offset(offset)
        data = self.make_reply_info(replies)
        return data

    @rpc
    def get_posting_info(self, posting_id):
        topic = None
        posting_type = ''
        target_posting = self.session.query(Posting).filter(Posting.posting_id == posting_id).first()
        if target_posting:
            posting_type = target_posting.posting_type
        else:
            target_posting = self.session.query(Reply).filter(Reply.posting_id == posting_id).first()
            if target_posting:
                posting_type = "discussion"
                topic = self.session.query(Posting).filter(
                    Posting.discussion_id == target_posting.discussion_id).first().posting_topic
        if target_posting:
            return {
                "posting_id": target_posting.posting_id,
                "posting_type": posting_type,
                "sender": target_posting.sender,
                "posting_time": target_posting.posting_time,
                "topic": topic if topic else target_posting.posting_topic,
                "message": target_posting.message,
                "posting_status": target_posting.posting_status
            }
        return None

    @rpc
    def search_posting(self, gid, topic, start_date, end_date, sender):
        posting_query = self.session.query(Posting) \
            .filter(Posting.posting_status == "open") \
            .filter(Posting.group_id == gid)
        if start_date:
            start_date = datetime.fromtimestamp(start_date)
            posting_query = posting_query.filter(Posting.posting_time >= start_date)
        if end_date:
            end_date = datetime.fromtimestamp(end_date)
            posting_query = posting_query.filter(Posting.posting_time <= end_date)
        if topic:
            posting_query = posting_query.filter(Posting.posting_topic.like("%" + topic + "%"))
        if sender:
            posting_query = posting_query.filter(Posting.sender == sender)
        result = posting_query.all()
        return self.make_posting_info(result)

    @rpc
    def search_replies(self, discussion_id, start_date, end_date, sender):
        posting_query = self.session.query(Reply).filter(Reply.discussion_id == discussion_id)
        if start_date:
            start_date = datetime.fromtimestamp(start_date)
            posting_query = posting_query.filter(Posting.posting_time >= start_date)
        if end_date:
            end_date = datetime.fromtimestamp(end_date)
            posting_query = posting_query.filter(Posting.posting_time <= end_date)
        if sender:
            posting_query = posting_query.filter(Posting.sender == sender)
        result = posting_query.all()
        return self.make_reply_info(result)

    @rpc
    def approve_posting(self, posting_id):
        target_posting = self.session.query(Posting).filter(Posting.posting_id == posting_id).first()
        if target_posting:
            event_id = target_posting.event_id
            with ClusterRpcProxy(CONFIG) as _rpc:
                if _rpc.event_service.approve(event_id):
                    target_posting.posting_status = "open"
                    _rpc.event_service.commit()
                    self.commit()
                    return True
        return False

    @rpc
    def reject_posting(self, posting_id):
        target_posting = self.session.query(Posting).filter(Posting.posting_id == posting_id).first()
        if target_posting:
            event_id = target_posting.event_id
            with ClusterRpcProxy(CONFIG) as _rpc:
                if _rpc.event_service.reject(event_id):
                    target_posting.posting_status = "rejected"
                    _rpc.event_service.commit()
                    self.commit()
                    return True
        return False

    @rpc
    def remove_a_posting(self, posting_id):
        deleted_posting = self.session.query(Reply).filter(Reply.posting_id == posting_id).first()
        if deleted_posting:
            self.session.delete(deleted_posting)
        else:
            deleted_posting = self.session.query(Posting).filter(Posting.posting_id == posting_id).first()
            if not deleted_posting:
                return None
            if deleted_posting.posting_type == 'dissemination':
                self.session.delete(deleted_posting)
            else:
                deleted_replies = self.session.query(Reply).filter(
                    Reply.discussion_id == deleted_posting.discussion_id).all()
                self.session.delete(deleted_posting)
                for reply in deleted_replies:
                    self.session.delete(reply)
        self.commit()
        return {
            "posting_id": deleted_posting.posting_id,
            "sender": deleted_posting.sender,
            "message": deleted_posting.message
        }

    @rpc
    def get_posting_list(self):
        posting_list = []
        with ClusterRpcProxy(CONFIG) as _rpc:
            posting_events = _rpc.event_service.get_all_events("posting")
            for p_event in posting_events:
                sender_info = _rpc.user_service.get_user_info(p_event['initiator'])
                posting_info = self.session.query(Posting).filter(Posting.event_id == p_event['event_id']).first()
                posting_list.append({
                    "eventID": p_event['event_id'],
                    "postingID": posting_info.posting_id,
                    "senderID": p_event['initiator'],
                    "senderName": sender_info['user_name'],
                    "senderEmail": sender_info['email'],
                    "posting_time": p_event['created_time'],
                    "topic": posting_info.posting_topic,
                    "message": posting_info.message
                })
            return posting_list

    @rpc
    def has_this_posting(self, posting_id, readable=True):
        posting = self.session.query(Posting) \
            .filter(Posting.posting_id == posting_id)
        if readable:
            posting = posting.filter(Posting.posting_status == 'open').first()
        if posting:
            return True
        reply = self.session.query(Reply).filter(Reply.posting_id == posting_id).first()
        if reply:
            return True
        return False

    @rpc
    def terminate_a_posting(self, posting_id):
        target = self.session.query(Posting).filter(Posting.posting_id == posting_id).first()
        target.posting_status = "terminated"
        self.commit()
        return True

    @rpc
    def commit(self):
        self.session.commit()

    @rpc
    def rollback(self):
        self.session.rollback()
