# encoding: utf-8
"""
@author: Ding Mengru
@contact: dingmengru1993@gmail.com

@version: 1.0
@file: tb_tf_record.py.py
@time: 04.12.19 13:58

新建数据表generate_tf_record.tf_record
记录/data/dmr/one_sent_per_line路径下的所有txt是否已经转成tf_record数据格式
"""
import os
import pandas as pd
from config import Config
from sqlalchemy.sql import text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, Integer, String

DATA_PATH = '/data/dmr/one_sent_per_line/'
Base = declarative_base()


def to_dict(self):
    return {c.name: getattr(self, c.name, None)
            for c in self.__table__.columns}


Base.to_dict = to_dict


class TFRecord(Base):
    __tablename__ = 'tf_record'

    desc = '记录每篇txt是否已经生成tf_record'

    TXT_ID = Column(Integer, primary_key=True, doc={'zh': 'TXT文本编号'})
    TXT_PATH = Column(String(600), doc={'zh': '处理成一行一句的TXT文本存储路径'})
    TXT_TITLE = Column(String(400), doc={'zh': 'TXT文本标题'})
    TXT_PROCESS = Column(String(1), server_default=text('0'), doc={'zh': '已生成tf_record的txt为1，未生成的为0'})

    def __repr__(self):
        return "<PDF2TXT14GB(TXT_ID='%d', TXT_PATH='%s', TXT_TITLE='%s', TXT_PROCESS='%s')>" % (
            self.TXT_ID, self.TXT_PATH, self.TXT_TITLE, self.TXT_PROCESS)

    @staticmethod
    def create_table():
        engine = create_engine(Config.GEN_TF_REC_SQL)
        TFRecord.__table__.drop(engine, checkfirst=True)
        Base.metadata.create_all(engine)

        txt_path_list = []
        txt_title_list = []
        txt_files = os.listdir(DATA_PATH)
        txt_count = 0
        for txt_file in txt_files:
            txt_path_name = DATA_PATH + txt_file
            print('已读入的txt文本名称：', txt_path_name)
            txt_path_list.append(txt_path_name)
            txt_title_list.append(txt_file)

        txt_count = txt_count + len(txt_path_list)
        txt_info = dict(TXT_PATH=txt_path_list, TXT_TITLE=txt_title_list)
        df = pd.DataFrame(txt_info)
        df.to_sql('tf_record', con=engine, if_exists='append', index=False)

        print('txt总篇数：', txt_count)  # txt_count:553071


if __name__ == '__main__':
    TFRecord.create_table()
