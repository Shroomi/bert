# encoding: utf-8
"""
@author: Ding Mengru
@contact: dingmengru1993@gmail.com

@version: 1.0
@file: multi_process_create.py.py
@time: 04.12.19 15:25

多线程跑run_create_pretraining_data.sh
"""
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import Config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from orm.tf_record.tb_tf_record import TFRecord

TXT_PATH = '/data/dmr/one_sent_per_line'
TF_RECORD_PATH = '/data/dmr/tf_record'
BERT_BASE_DIR = '/home/dingmengru/workspace/bert'
DATA_DIR = '/data/dmr'
TXT_COUNT = 553071

engine = create_engine(Config.GEN_TF_REC_SQL)
db_session = sessionmaker(bind=engine)


def run_create_pretraining_data(txt_name, txt_id):
    python_interpreter = '/home/dingmengru/.conda/envs/tensorflow-py36/bin/python'
    python_file = os.path.join(BERT_BASE_DIR, 'bert/create_pretraining_data.py')
    input_file = os.path.join(TXT_PATH, txt_name)
    output_file = os.path.join(TF_RECORD_PATH, txt_name.replace('.txt', '.tfrecord'))
    vocab_file = os.path.join(BERT_BASE_DIR, 'chinese_L-12_H-768_A-12/vocab.txt')

    cmd = [python_interpreter, python_file, '--input_file={}'.format(input_file),
           '--output_file={}'.format(output_file), '--vocab_file={}'.format(vocab_file), '--max_seq_length=512',
           '--max_predictions_per_seq=77', '--masked_lm_prob=0.15', '--random_seed=12345', '--dupe_factor=5']
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    out, err = proc.communicate()
    # if out:
    #     print(out.decode('utf-8'))

    session = db_session()
    print('要完成的')
    print(txt_id)
    session.query(TFRecord).filter(TFRecord.TXT_ID == str(txt_id)).update({'TXT_PROCESS': '1'})
    session.commit()
    session.close()


def run():
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_list = []
        for txt_id in range(1, (TXT_COUNT+1)):
            session = db_session()
            txt_info = session.query(TFRecord).filter(TFRecord.TXT_ID == str(txt_id)).first()
            txt_name, txt_status = txt_info.TXT_TITLE, txt_info.TXT_PROCESS
            session.close()
            if txt_status == '0':
                print('将要提交的任务')
                print(txt_id)
                future = executor.submit(run_create_pretraining_data, txt_name, txt_id)
                future_list.append(future)

    for future in as_completed(future_list):
        res = future.result()


if __name__ == '__main__':
    run()
