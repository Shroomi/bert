#!/usr/bin/env bash

export BERT_BASE_DIR=/home/dingmengru/workspace/bert
export DATA_DIR=/data/dmr

python run_pretraining.py \
    --input_file=$DATA_DIR/test_tf_record/01湖南富兴集团有限公司2014年第二期短期融资券募集说明书.tfrecord \
    --output_dir=$BERT_BASE_DIR/pretraining_results/output_test \
    --do_train=True \
    --do_eval=True \
    --bert_config_file=$BERT_BASE_DIR/chinese_L-12_H-768_A-12/bert_config.json \
    --train_batch_size=8 \
    --max_seq_length=512 \
    --max_predictions_per_seq=77 \
    --num_train_steps=20 \
    --num_warmup_steps=10 \
    --learning_rate=2e-5
