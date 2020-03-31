# coding=utf-8
from nameko.rpc import rpc
from sqlalchemy import Column, Text, Integer
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()
engine = create_engine('sqlite:///../dict.db')
Session = sessionmaker()
Session.configure(bind=engine)


class Keyword(Base):
    __tablename__ = "keywords"

    id = Column(Integer, primary_key=True, autoincrement=True)
    keyword = Column(Text, nullable=False, unique=True)


class KeywordService(object):
    name = "keyword_service"
    querySession = Session()

    @classmethod
    def has_this_keyword(cls, keyword):
        return cls.querySession.query(Keyword).filter(Keyword.keyword == keyword.lower()).first()

    @rpc
    def get_all_keywords(self):
        keywords = []
        data = self.querySession.query(Keyword).all()
        for entry in data:
            keywords.append(entry.keyword)
        return keywords

    @rpc
    def add_a_keyword(self, keyword):
        session = Session()
        if self.has_this_keyword(keyword) is not None:
            return False
        new_word = Keyword(keyword=keyword.lower())
        session.add(new_word)
        session.commit()
        return True

    @rpc
    def remove_a_keyword(self, keyword):
        session = Session()
        if self.has_this_keyword(keyword) is None:
            return False
        deleted_word = self.querySession.query(Keyword).filter(Keyword.keyword == keyword.lower()).first()
        session.delete(deleted_word)
        session.commit()
        return True

    @rpc
    def check_discussion_posting(self, message, topic=None):
        for word in message.split(" "):
            match = self.querySession.query(Keyword).filter(Keyword.keyword == word.lower()).first()
            if match is not None:
                return True
        if topic:
            for word in topic.split(" "):
                match = self.querySession.query(Keyword).filter(Keyword.keyword == word.lower()).first()
                if match is not None:
                    return True
        return False
