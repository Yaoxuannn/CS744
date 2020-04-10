# coding=utf-8
from datetime import datetime
from hashlib import md5
from time import time

from nameko.rpc import rpc
from nameko.standalone.rpc import ClusterRpcProxy
from sqlalchemy import Column, Integer, Text, DateTime, or_, func
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


class PrivateConversation(Base):
    __tablename__ = "private_conversation"

    conversation_id = Column(Text, primary_key=True)
    event_id = Column(Text)
    patient_id = Column(Text, nullable=False)
    patient_valid = Column(Integer, default=0)
    physician_id = Column(Text, nullable=False)
    physician_valid = Column(Integer, default=0)
    password = Column(Text, nullable=False)
    posting_time = Column(DateTime)
    topic = Column(Text)
    message = Column(Text)
    status = Column(Text)


class PrivateMessage(Base):
    __tablename__ = "private_message"

    message_id = Column(Text, primary_key=True)
    conversation_id = Column(Text)
    sender = Column(Text, nullable=False)
    posting_time = Column(DateTime)
    message = Column(Text)


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
    querySession = Session()

    @staticmethod
    def generate_posting_id():
        now = datetime.now().strftime("%Y%m%d")
        md5_obj = md5()
        md5_obj.update(str(time()).encode())
        return "p" + now + md5_obj.hexdigest()[:7]

    @staticmethod
    def generate_conversation_id(patient_id, physician_id):
        md5_obj = md5()
        md5_obj.update(str(time()).encode())
        md5_obj.update(str(patient_id).encode())
        md5_obj.update(str(physician_id).encode())
        return "c" + md5_obj.hexdigest()[:15]

    @staticmethod
    def generate_onetime_password():
        md5_obj = md5()
        md5_obj.update(str(time()).encode())
        return "X" + md5_obj.hexdigest[:6] + "X"

    @staticmethod
    def generate_discussion_id():
        now = datetime.now().strftime("%Y%m%d")
        md5_obj = md5()
        md5_obj.update(str(time()).encode())
        return 'd' + now + md5_obj.hexdigest()[:12]

    @rpc
    def add_private_conversation(self, patient_id, physician_id, topic, message):
        session = Session()
        with ClusterRpcProxy(CONFIG) as _rpc:
            if _rpc.keyword_service.check_discussion_posting(message, topic) is False:
                return False
            new_conversation = PrivateConversation(
                conversation_id=self.generate_conversation_id(patient_id, physician_id),
                patient_id=patient_id,
                physician_id=physician_id,
                posting_time=datetime.now(),
                topic=topic,
                password=self.generate_onetime_password(),
                message=message,
                status="processing"
            )
            event_id = _rpc.event_service.add_event(
                event_type="private_request",
                initiator=patient_id,
                target=physician_id,
            )
            new_conversation.event_id = event_id
            session.add(new_conversation)
            session.commit()
            return True

    @rpc
    def send_private_message(self, conversation_id, sender_id, message):
        session = Session()
        with ClusterRpcProxy(CONFIG) as _rpc:
            if _rpc.keyword_service.check_discussion_posting(message) is False:
                return False
            new_private_message = PrivateMessage(
                message_id=self.generate_posting_id(),
                conversation_id=conversation_id,
                sender=sender_id,
                posting_time=datetime.now(),
                message=message
            )
            session.add(new_private_message)
            session.commit()
            return {
                "messageID": new_private_message.message_id,
                "posting_time": new_private_message.posting_time
            }

    @rpc
    def validate_password(self, user_id, conversation_id, password):
        session = Session()
        target = session.query(PrivateConversation).filter(
            PrivateConversation.conversation_id == conversation_id).first()
        if not target:
            return None
        if target.password == password:
            if target.patient_id == user_id:
                target.patient_valid = 1
            if target.physician_id == user_id:
                target.physician_valid = 1
            session.commit()
            return True
        return False

    @rpc
    def approve_conversation(self, event_id):
        session = Session()
        target = session.query(PrivateConversation).filter(PrivateConversation.event_id == event_id).first()
        if not target:
            return False
        with ClusterRpcProxy(CONFIG) as _rpc:
            _rpc.event_service.approve(event_id)
            target.status = 'open'
            session.commit()
            event_info = _rpc.event_service.get_event_info(event_id)
            patient_info = _rpc.user_service.get_user_info(event_info['initiator'])
            physician_info = _rpc.user_service.get_user_info(event_info['target'])
            _rpc.mail_service.send_mail(patient_info['email'], "APPROVED: Private Conversation Request",
                                        "<b>Request approved</b><br/>Your private conversation request to {} is "
                                        "approved by the administrator.<br/>One-time password: <b>{}</b>".format(
                                            physician_info['user_name'], target.password
                                        ))
            return True

    @rpc
    def reject_conversation(self, event_id):
        session = Session()
        target = session.query(PrivateConversation).filter().first()
        if not target:
            return False
        with ClusterRpcProxy(CONFIG) as _rpc:
            _rpc.event_service.reject(event_id)
            target.status = 'rejected'
            session.commit()
            event_info = _rpc.event_service.get_event_info(event_id)
            patient_info = _rpc.user_service.get_user_info(event_info['initiator'])
            physician_info = _rpc.user_service.get_user_info(event_info['target'])
            _rpc.mail_service.send_mail(patient_info['email'], "REJECTED: Private Conversation Request",
                                        "<b>Request Rejected</b><br/>We are sorry to tell you that your private "
                                        "conversation request to %s is rejected by the administrator" %
                                        physician_info['user_name'])
            return True

    @rpc
    def add_posting(self, sender_id, posting_type, topic, message, gid):
        session = Session()
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
        session.add(new_posting)
        session.commit()
        if posting_type == 'dissemination':
            return True
        return event_id, new_posting.discussion_id, new_posting.posting_time

    @rpc
    def get_dissemination(self, group_id):
        postings = self.querySession.query(Posting) \
            .filter(Posting.posting_type == 'dissemination') \
            .filter(Posting.group_id == group_id) \
            .order_by(Posting.posting_time.desc()) \
            .all()
        data = []
        with ClusterRpcProxy(CONFIG) as _rpc:
            for posting in postings:
                user_info = _rpc.user_service.get_user_info(posting.sender)
                data.append({
                    "postingID": posting.posting_id,
                    "postingType": posting.posting_type,
                    "topic": posting.posting_topic,
                    "senderID": posting.sender,
                    "groupID": group_id,
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
    def get_discussions(self, group_id):
        postings = self.querySession.query(Posting) \
            .filter(Posting.posting_type == 'discussion') \
            .filter(Posting.group_id == group_id) \
            .filter(or_(Posting.posting_status == "open", Posting.posting_status == "terminated")) \
            .order_by(Posting.posting_time.desc()) \
            .all()
        data = self.make_posting_info(postings)
        return data

    @rpc
    def get_private_conversation(self, user_id):
        data = []
        with ClusterRpcProxy(CONFIG) as _rpc:
            conversation_list = self.querySession.query(PrivateConversation) \
                .filter(or_(PrivateConversation.patient_id == user_id,
                            PrivateConversation.physician_id == user_id)) \
                .filter(PrivateConversation.status == 'open') \
                .all()
        for c_item in conversation_list:
            with ClusterRpcProxy(CONFIG) as _rpc:
                patient_info = _rpc.user_service.get_user_info(c_item.patient_id)
                physician_info = _rpc.user_service.get_user_info(c_item.physician_id)
            data.append({
                "conversationID": c_item.conversation_id,
                "patientID": c_item.patient_id,
                "patientName": patient_info["user_name"],
                "physicianID": c_item.physician_id,
                "physicianName": physician_info["user_name"],
                "posting_time": c_item.posting_time.strftime("%m/%d/%Y %H:%M %p"),
                "topic": c_item.topic,
                "message": c_item.message,
                "status": c_item.status
            })
        return data

    @rpc
    def get_conversation_message(self, conversation_id):
        data = []
        messages = self.querySession.query(PrivateMessage) \
            .filter(PrivateMessage.conversation_id == conversation_id) \
            .order_by(PrivateMessage.posting_time.asc()) \
            .all()
        for msg in messages:
            with ClusterRpcProxy(CONFIG) as _rpc:
                sender_info = _rpc.user_service.get_user_info(msg.sender)
            data.append({
                "messageID": msg.message_id,
                "senderID": msg.sender,
                "senderName": sender_info['user_name'],
                "posting_time": msg.posting_time.strftime("%m/%d/%Y %H:%M %p"),
                "message": msg.message
            })

    @rpc
    def terminate_private_conversation(self, conversation_id):
        session = Session()
        target = session.query(PrivateConversation).filter(
            PrivateConversation.conversation_id == conversation_id).first()
        target.posting_status = "terminated"
        session.commit()
        return True

    @rpc
    def logout_private_conversations(self, user_id):
        session = Session()
        conversations = session.query(PrivateConversation) \
            .filter(or_(PrivateConversation.patient_id == user_id, PrivateConversation.physician_id == user_id)).all()
        for c in conversations:
            self.terminate_private_conversation(c.conversation_id)

    @rpc
    def get_conversation_status(self, conversation_id):
        target = self.querySession.query(PrivateConversation).filter(
            PrivateConversation.conversation_id == conversation_id).first()
        if not target:
            return None
        return {
            "status": target.status,
            "patient_valid": target.patient_valid,
            "physician_valid": target.physician_valid
        }

    @rpc
    def reply(self, sender_id, discussion_id, message):
        session = Session()
        discussion_posting = self.querySession.query(Posting).filter(Posting.discussion_id == discussion_id).first()
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
        session.add(new_reply)
        session.commit()
        return new_reply.posting_id, new_reply.posting_time.strftime("%m/%d/%Y %H:%M %p")

    @rpc
    def get_replies(self, discussion_id, limit=8, offset=0):
        replies = self.querySession.query(Reply) \
            .filter(Reply.discussion_id == discussion_id) \
            .order_by(Reply.posting_time.asc()) \
            .limit(limit) \
            .offset(offset)
        data = self.make_reply_info(replies)
        return data

    @rpc
    def get_posting_info(self, posting_id):
        data = {}
        topic = None
        posting_type = ''
        posting_status = ''
        target_posting = self.querySession.query(Posting).filter(Posting.posting_id == posting_id).first()
        if target_posting:
            posting_status = target_posting.posting_status
            posting_type = target_posting.posting_type
            if posting_type == 'discussion':
                data.update({
                    "discussion_id": target_posting.discussion_id,
                    "group_id": target_posting.group_id
                })
        else:
            target_posting = self.querySession.query(Reply).filter(Reply.posting_id == posting_id).first()
            if target_posting:
                posting_type = "discussion"
                posting_status = ""
                topic = self.querySession.query(Posting).filter(
                    Posting.discussion_id == target_posting.discussion_id).first().posting_topic
        if target_posting:
            data.update({
                "posting_id": target_posting.posting_id,
                "posting_type": posting_type,
                "sender": target_posting.sender,
                "posting_time": target_posting.posting_time.strftime("%m/%d/%Y %H:%M %p"),
                "topic": topic if topic else target_posting.posting_topic,
                "message": target_posting.message,
                "posting_status": posting_status
            })
            return data
        return None

    @rpc
    def search_posting(self, gid, topic, start_date, end_date, sender):
        posting_query = self.querySession.query(Posting) \
            .filter(or_(Posting.posting_status == "open", Posting.posting_status == "terminated")) \
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
        posting_query = self.querySession.query(Reply).filter(Reply.discussion_id == discussion_id)
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
        session = Session()
        target_posting = session.query(Posting).filter(Posting.posting_id == posting_id).first()
        if target_posting:
            event_id = target_posting.event_id
            with ClusterRpcProxy(CONFIG) as _rpc:
                if _rpc.event_service.approve(event_id):
                    target_posting.posting_status = "open"
                    session.commit()
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
                    target_posting.posting_status = "rejected"
                    session.commit()
                    return True
        return False

    @rpc
    def remove_a_posting(self, posting_id):
        session = Session()
        deleted_posting = session.query(Reply).filter(Reply.posting_id == posting_id).first()
        if deleted_posting:
            session.delete(deleted_posting)
        else:
            deleted_posting = session.query(Posting).filter(Posting.posting_id == posting_id).first()
            if not deleted_posting:
                return None
            if deleted_posting.posting_type == 'dissemination':
                session.delete(deleted_posting)
            else:
                deleted_replies = session.query(Reply).filter(
                    Reply.discussion_id == deleted_posting.discussion_id).all()
                session.delete(deleted_posting)
                for reply in deleted_replies:
                    session.delete(reply)
        session.commit()
        return {
            "posting_id": deleted_posting.posting_id,
            "sender": deleted_posting.sender,
            "message": deleted_posting.message
        }

    @rpc
    def get_posting_list(self):
        posting_list = []
        private_list = []
        with ClusterRpcProxy(CONFIG) as _rpc:
            posting_events = _rpc.event_service.get_all_events("posting")
            private_events = _rpc.event_service.get_all_events("private_request")
            for p_event in posting_events:
                sender_info = _rpc.user_service.get_user_info(p_event['initiator'])
                posting_info = self.querySession.query(Posting).filter(Posting.event_id == p_event['event_id']).first()
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
            for c_event in private_events:
                conversation_info = self.querySession.query(PrivateConversation).filter(
                    PrivateConversation.event_id == c_event['event_id']).first()
                patient_info = _rpc.user_service.get_user_info(c_event['initiator'])
                physician_info = _rpc.user_service.get_user_info(c_event['target'])
                private_list.append({
                    "eventID": c_event['event_id'],
                    "conversationID": conversation_info.conversation_id,
                    "patientID": c_event['initiator'],
                    "patientName": patient_info['user_name'],
                    "patientEmail": patient_info['email'],
                    "physicianID": c_event['target'],
                    "physicianName": physician_info['user_name'],
                    "physicianEmail": physician_info['email'],
                    "request_time": c_event['created_time']
                })
            return posting_list, private_list

    @rpc
    def has_this_posting(self, posting_id):
        posting = self.querySession.query(Posting) \
            .filter(Posting.posting_id == posting_id)
        if posting:
            return True
        reply = self.querySession.query(Reply).filter(Reply.posting_id == posting_id).first()
        if reply:
            return True
        return False

    @rpc
    def terminate_a_posting(self, posting_id):
        session = Session()
        target = session.query(Posting).filter(Posting.posting_id == posting_id).first()
        target.posting_status = "terminated"
        session.commit()
        return True

    @rpc
    def counting_info(self, user_id, start_time, end_time):

        total_dissemination = self.querySession.query(Posting) \
            .filter(Posting.posting_time > start_time) \
            .filter(Posting.posting_time < end_time) \
            .filter(Posting.posting_type == 'dissemination')
        n_total_dissemination = self.get_count(total_dissemination)
        n_user_dissemination = self.get_count(total_dissemination.filter(Posting.sender == user_id))
        total_discussion = self.querySession.query(Posting) \
            .filter(Posting.posting_time > start_time) \
            .filter(Posting.posting_time < end_time) \
            .filter(Posting.posting_type == 'discussion') \
            .filter(Posting.posting_status == 'open')
        n_total_discussion = self.get_count(total_discussion)
        n_user_discussion = self.get_count(total_discussion.filter(Posting.sender == user_id))
        n_involved_user = self.get_count(self.querySession.query(Reply.sender)
                                         .filter(Reply.posting_time > start_time)
                                         .filter(Reply.posting_time < end_time)
                                         .distinct())
        return {
            "total_dissemination": n_total_dissemination,
            "total_discussion": n_total_discussion,
            "user_dissemination": n_user_dissemination,
            "user_discussion": n_user_discussion,
            "involved_user": n_involved_user
        }

    @staticmethod
    def get_count(q):
        count_q = q.statement.with_only_columns([func.count()]).order_by(None)
        count = q.session.execute(count_q).scalar()
        return count
