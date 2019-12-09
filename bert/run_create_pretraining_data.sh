# 将/data/dmr/one_sent_per_line下的数据转换成tf_record数据类型，存放在/data/dmr/tf_record下

export BERT_BASE_DIR=/home/dingmengru/workspace/bert
export DATA_DIR=/data/dmr

python create_pretraining_data.py \
	--input_file=$DATA_DIR/one_sent_per_line/38389331.txt \
	--output_file=$DATA_DIR/tf_record/38389331.tfrecord \
	--vocab_file=$BERT_BASE_DIR/chinese_L-12_H-768_A-12/vocab.txt \
	--max_seq_length=512 \
	--max_predictions_per_seq=77 \
	--masked_lm_prob=0.15 \
	--random_seed=12345 \
	--dupe_factor=5
