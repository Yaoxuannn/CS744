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
    name = "dict_service"
    session = Session()

    @classmethod
    def has_this_keyword(cls, keyword):
        return cls.session.query(Keyword).filter(Keyword.keyword == keyword.lower()).first()

    @rpc
    def get_all_keywords(self):
        return self.session.query(Keyword.keyword).all()

    @rpc
    def add_a_keyword(self, keyword):
        if self.has_this_keyword(keyword) is not None:
            return False
        new_word = Keyword(keyword=keyword.lower())
        self.session.add(new_word)
        self.commit()
        return True

    @rpc
    def remove_a_keyword(self, keyword):
        if self.has_this_keyword(keyword) is None:
            return False
        deleted_word = self.session.query(Keyword).filter(Keyword.keyword == keyword.lower()).first()
        self.session.delete(deleted_word)
        self.commit()
        return True

    @rpc
    def rollback(self):
        self.session.rollback()

    @rpc
    def commit(self):
        self.session.commit()
