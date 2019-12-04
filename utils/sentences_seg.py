# encoding: utf-8
"""
@author: Ding Mengru
@contact: dingmengru1993@gmail.com

@version: 2.0
@file: sentences_seg.py
@time: 29.11.19 20:56

处理pdf2txt转出的txt文本:
1，去掉长度较短的句子(原因：句子过短的时候，mask之后会有很多种可能，任务就会变得很困难，
比如'行业风险'，mask掉'险'，'行业风向'也对，'行业风投'也对)
2，去掉不通顺的，有错的句子(公司内部pdf转txt接口前期转出的txt很乱，很难去掉有错的句子)
3，一句一行进行分割

路径说明:
1，未处理的txt有3处来源：(1) 最先pdf2txt转出的14GB的txt(/data/dmr/success_txt) 已完成
                     (2) 第2轮pdf2txt转出的16GB的txt(/data/dmr/text_16G) 在进行中
                     (3) 从sql中恢复的40GB的txt(/data/dmr/text_finnews) 在进行中
2，处理完成后的txt存储路径为：/data/dmr/one_sent_per_line

2019年12月03日(已完成)：
先将第(1)部分的txt数据处理后放入/data/dmr/one_sent_per_line
步骤1，将第(1)部分的txt数据不分文件夹地全部写入数据表txt_clean.pdf2txt_14GB，并记录处理状态，防止程序意外停下
步骤2，处理第(1)部分的txt文件，写入到/data/dmr/one_sent_per_line路径下

ing：等待第(2)，(3)部分数据完成
"""
import os
import re
from config import Config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from orm.pdf2txt_14GB.tb_pdf2txt_14GB import PDF2TXT14GB

TXT_COUNT = 562396
OUTPUT_PATH = '/data/dmr/one_sent_per_line/'
if not os.path.exists(OUTPUT_PATH):
    os.mkdir(OUTPUT_PATH)


def process_txt(txt_file, title, text_id):
    print('clean {}: {}'.format(text_id, txt_file))
    output_file = OUTPUT_PATH + title
    write_file = open(output_file, 'a', encoding='utf-8')
    with open(txt_file, 'r', encoding='utf-8') as in_file:
        for line in in_file:
            line = line.strip()
            # 去掉较短的句子
            if len(line) <= 5:
                continue
            # 去掉表格
            if 'table' in line:
                continue

            # 将句子分开要考虑的情况：
            # 1，一行超过2句话；2，一行多个句子之间的分割的标点符号不同；
            # 3，两个标点符号连在一起的情况(留下第1个标点符号)
            sentences_list = re.split('([。！？;]+)', line)
            sent_list_len = len(sentences_list)
            if sent_list_len > 1:
                i = 0
                while i <= (sent_list_len - 3):
                    write_file.write(sentences_list[i]+sentences_list[i+1][0]+'\n')
                    i = i + 2
            # sentences的长度为1的时候，说明txt文档本身，句子就不带有标点，直接写入文件
            else:
                write_file.write(sentences_list[0]+'\n')

        write_file.close()


engine = create_engine(Config.TXT_CLEAN_SQL)
db_session = sessionmaker(bind=engine)
session = db_session()

for txt_id in range(1, (TXT_COUNT + 1)):
    txt_info = session.query(PDF2TXT14GB).filter(PDF2TXT14GB.TXT_ID == str(txt_id)).first()
    txt_info = txt_info.to_dict()
    # txt_path: /data/dmr/success_txt/cninfo_announcement_category_list_txt/xxxx.txt
    # txt_title: xxxx.txt
    txt_path = txt_info['TXT_PATH']
    txt_title = txt_info['TXT_TITLE']
    txt_status = txt_info['TXT_PROCESS']
    if txt_status == '0':
        # 处理txt文本
        process_txt(txt_path, txt_title, txt_id)
        # 将TXT_PROCESS改成'1'
        session.query(PDF2TXT14GB).filter(PDF2TXT14GB.TXT_ID == str(txt_id)).update({'TXT_PROCESS': '1'})
        session.commit()
    else:
        print('已处理 {}: {}'.format(txt_id, txt_path))

    session.close()
