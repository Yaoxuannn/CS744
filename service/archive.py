# coding=utf-8
from datetime import datetime

from nameko.rpc import rpc
from nameko.standalone.rpc import ClusterRpcProxy
from sqlalchemy import Column, Text, DateTime
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()
engine = create_engine('sqlite:///../archive.db')
Session = sessionmaker()
Session.configure(bind=engine)

CONFIG = {'AMQP_URI': "amqp://guest:guest@localhost"}


class ArchivedPosting(Base):
    __tablename__ = "archived_posting"

    posting_id = Column(Text, primary_key=True)
    sender = Column(Text, nullable=False)
    posting_time = Column(DateTime)
    posting_topic = Column(Text)
    message = Column(Text, nullable=False)
    group_id = Column(Text, nullable=False)
    discussion_id = Column(Text)


class ArchivedReply(Base):
    __tablename__ = "archived_reply"

    posting_id = Column(Text, primary_key=True)
    discussion_id = Column(Text, nullable=False)
    sender = Column(Text, nullable=False)
    posting_time = Column(DateTime)
    message = Column(Text, nullable=False)


class ArchiveService(object):
    name = "archive_service"
    querySession = Session()

    @rpc
    def archive(self, posting_id):
        session = Session()
        with ClusterRpcProxy(CONFIG) as _rpc:
            posting = _rpc.posting_service.get_posting_info(posting_id)
            if posting is None:
                return None
            if posting['posting_type'] != 'discussion':
                return None
            new_archived_posting = ArchivedPosting(
                posting_id=posting['posting_id'],
                sender=posting['sender'],
                posting_time=datetime.strptime(posting['posting_time'], "%m/%d/%Y %H:%M %p"),
                posting_topic=posting['topic'],
                message=posting['message'],
                group_id=posting['group_id'],
                discussion_id=posting['discussion_id']
            )
            session.add(new_archived_posting)
            replies = _rpc.posting_service.get_replies(posting['discussion_id'], limit=9999)
            for reply in replies:
                session.add(ArchivedReply(
                    posting_id=reply['postingID'],
                    discussion_id=posting['discussion_id'],
                    sender=reply['senderID'],
                    posting_time=datetime.strptime(reply['posting_time'], "%m/%d/%Y %H:%M %p"),
                    message=reply['message']
                ))
            session.commit()
            result = _rpc.posting_service.remove_a_posting(posting_id)
            return result['posting_id']

    @staticmethod
    def make_posting_info(postings):
        data = []
        with ClusterRpcProxy(CONFIG) as _rpc:
            for posting in postings:
                user_info = _rpc.user_service.get_user_info(posting.sender)
                data.append({
                    "postingID": posting.posting_id,
                    "groupID": posting.group_id,
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
    def search_archived_posting(self, topic, start_date, end_date, sender):
        posting_query = self.querySession.query(ArchivedPosting)
        if start_date:
            start_date = datetime.fromtimestamp(start_date)
            posting_query = posting_query.filter(ArchivedPosting.posting_time >= start_date)
        if end_date:
            end_date = datetime.fromtimestamp(end_date)
            posting_query = posting_query.filter(ArchivedPosting.posting_time <= end_date)
        if topic:
            posting_query = posting_query.filter(ArchivedPosting.posting_topic.like("%" + topic + "%"))
        if sender:
            posting_query = posting_query.filter(ArchivedPosting.sender == sender)
        result = posting_query.all()
        return self.make_posting_info(result)

    @rpc
    def get_replies(self, discussion_id):
        replies = self.querySession.query(ArchivedReply) \
            .filter(ArchivedReply.discussion_id == discussion_id) \
            .order_by(ArchivedReply.posting_time.asc()).all()
        data = self.make_reply_info(replies)
        return data
