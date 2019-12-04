# encoding: utf-8
"""
@author: Ding Mengru
@contact: dingmengru1993@gmail.com

@version: 1.0
@file: tb_pdf2txt_14GB.py.py
@time: 03.12.19 11:53

新建数据表txt_clean.pdf2txt_14GB
记录/data/dmr/success_txt路径下的所有txt的处理状态
"""
import os
import pandas as pd
from config import Config
from sqlalchemy.sql import text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, Integer, String

DATA_PATH = '/data/dmr/success_txt/'
Base = declarative_base()


def to_dict(self):
    return {c.name: getattr(self, c.name, None)
            for c in self.__table__.columns}


Base.to_dict = to_dict


class PDF2TXT14GB(Base):
    __tablename__ = 'pdf2txt_14GB'

    desc = '记录每篇txt是否被处理过(被处理成生成tf_record需要的文本样式)'

    TXT_ID = Column(Integer, primary_key=True, doc={'zh': 'TXT文本编号'})
    TXT_PATH = Column(String(600), doc={'zh': 'TXT原始文本存储路径'})
    TXT_TITLE = Column(String(400), doc={'zh': 'TXT文本标题'})
    TXT_PROCESS = Column(String(1), server_default=text('0'), doc={'zh': 'PDF被处理过为1，未被处理过为0'})

    def __repr__(self):
        return "<PDF2TXT14GB(TXT_ID='%d', TXT_PATH='%s', TXT_TITLE='%s', TXT_PROCESS='%s')>" % (
            self.TXT_ID, self.TXT_PATH, self.TXT_TITLE, self.TXT_PROCESS)

    @staticmethod
    def create_table():
        engine = create_engine(Config.TXT_CLEAN_SQL)
        PDF2TXT14GB.__table__.drop(engine, checkfirst=True)
        Base.metadata.create_all(engine)

        txt_path_list = []
        txt_title_list = []
        folders = os.listdir(DATA_PATH)
        txt_count = 0
        for folder_name in folders:
            if folder_name.startswith('test'):
                continue
            txt_path = DATA_PATH + folder_name
            txt_files = os.listdir(txt_path)
            for txt_file in txt_files:
                txt_path_name = txt_path + '/' + txt_file
                print('已读入的txt文本名称：', txt_path_name)
                txt_path_list.append(txt_path_name)
                txt_title_list.append(txt_file)

            txt_count = txt_count + len(txt_path_list)
            txt_info = dict(TXT_PATH=txt_path_list, TXT_TITLE=txt_title_list)
            df = pd.DataFrame(txt_info)
            df.to_sql('pdf2txt_14GB', con=engine, if_exists='append', index=False)
            txt_path_list = []
            txt_title_list = []

        print('txt总篇数：', txt_count)  # txt_count: 562396


if __name__ == '__main__':
    PDF2TXT14GB.create_table()
