# encoding: utf-8
"""
@author: Ding Mengru
@contact: dingmengru1993@gmail.com

@version: 1.0
@file: text_classification_rnn.py
@time: 26.11.19 17:11

写一个简单的RNN文本分类，使用gpu进行模型训练，以测试tf-gpu环境是否可以正常运行。
使用数据：IMDB large movie review dataset
"""
import os
import tensorflow as tf
import tensorflow_datasets as tfds

# # 测试是否可以正常使用gpu的一种方式，如果输出gpu的相关信息，说明tf-gpu环境配置正确
# sess = tf.Session(config=tf.ConfigProto(log_device_placement=True))

# 指定使用第3号gpu
os.environ['CUDA_VISIBLE_DEVICES'] = '3'

# IMDB数据集是二分类数据集，分为positive和negative sentiment
dataset, info = tfds.load('imdb_reviews/subwords8k', with_info=True, as_supervised=True)
train_dataset, test_dataset = dataset['train'], dataset['test']

encoder = info.features['text'].encoder
print('Vocabulary size: {}'.format(encoder.vocab_size))

BUFFER_SIZE = 10000
BATCH_SIZE = 64

# 按buffer_size对训练数据进行洗牌
# 使用padded_batch方法，给句子序列补0(zero-pad the sequences)，使得句子长度都与最长句子等长
train_dataset = train_dataset.shuffle(BUFFER_SIZE)
train_dataset = train_dataset.padded_batch(BATCH_SIZE, train_dataset.output_shapes)
test_dataset = test_dataset.padded_batch(BATCH_SIZE, test_dataset.output_shapes)

# 建模(记得tensorboard看一下模型结构，确定和自己想的一不一样)
model = tf.keras.Sequential([
    tf.keras.layers.Embedding(encoder.vocab_size, 64),
    tf.keras.layers.Bidirectional(tf.keras.layers.LSTM(64)),
    tf.keras.layers.Dense(64, activation='relu'),
    tf.keras.layers.Dense(1, activation='sigmoid')
])
model.compile(
    loss='binary_crossentropy',
    optimizer=tf.keras.optimizers.Adam(1e-4),
    metrics=['accuracy']
)

# 训练模型
history = model.fit(train_dataset, epochs=10,
                    validation_data=test_dataset,
                    validation_steps=30, steps_per_epoch=30)
test_loss, test_acc = model.evaluate(test_dataset)
print('Test Loss: {}'.format(test_loss))
print('Test Accuracy: {}'.format(test_acc))
