# encoding: utf-8
"""
@author: Ding Mengru
@contact: dingmengru1993@gmail.com

@version: 1.0
@file: sql_state_back.py.py
@time: 06.12.19 10:13

将数据表的状态值归回到初始状态 0
"""
from config import Config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from orm.tf_record.tb_tf_record import TFRecord

engine = create_engine(Config.GEN_TF_REC_SQL)
db_session = sessionmaker(bind=engine)
session = db_session()
# 将数据表中的状态值归 0
session.query(TFRecord).filter(TFRecord.TXT_PROCESS == '1').update({'TXT_PROCESS': '0'})
session.commit()
session.close()
