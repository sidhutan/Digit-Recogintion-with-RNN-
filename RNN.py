# This python module is a computation graph

import tensorflow as tf
# imported the data sets from tensorflow
from tensorflow.examples.tutorials.mnist import input_data
from tensorflow.contrib import rnn
from tensorflow.python.ops import rnn_cell

# reading the data set
mnist = input_data.read_data_sets("/tmp/data", one_hot=True)

hm_epochs = 10  # pronouced as epics

n_classes = 10
# [0-9 digits recognise]

# Setting up the batch size, as 128 ie at a time 128 chunck of data will be taken
n_chunks = 28
chunk_size = 28
rnn_size = 128
batch_size = 128

# height x width
x = tf.placeholder('float', [None, n_chunks, chunk_size])  # x is input
y = tf.placeholder('float')


def recurrent_neural_network(x):
    """AdamOptimiser is used an optimiser 
        Weight's tensor is of rnn_size """
    layer = {'weight': tf.Variable(tf.random_normal([rnn_size, n_classes])),
             'biases': tf.Variable(tf.random_normal([n_classes]))}
    #shaping of the date so that it can be fed to RNN cell (LSTM)
    x = tf.transpose(x, [1, 0, 2])
    x = tf.reshape(x, [-1, chunk_size])
    x = tf.split(x, n_chunks, 0)
    # LSTM Networks. Long Short Term Memory networks – usually just called “LSTMs” – are a special kind of RNN, capable of learning long-term dependencies
    lstm_cell = rnn_cell.BasicLSTMCell(rnn_size)
    outputs, states = rnn.static_rnn(lstm_cell, x, dtype=tf.float32)
    output = tf.matmul(outputs[-1], layer['weight'])+ layer['biases']

    return output



def train_neural_network(x):
    # RNN's output will be saved in prediction
    prediction = recurrent_neural_network(x)
    #cost function 
    cost = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(logits=prediction,labels=y))
    # cost= tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(prediction,y))-> this was not  working in tensorflow1.6
    # by_default learning rate is 0.0001
    optimizer = tf.train.AdamOptimizer().minimize(cost)
    # cycle feed forward + backprop


    with tf.Session() as sess:
        sess.run(tf.global_variables_initializer())
        # sess.run(tf.initialize_all_variables())

        for epoch in range(hm_epochs):
            epoch_loss = 0
            # "_" variable we are not sure about.

            for _ in range(int(mnist.train.num_examples / batch_size)):
                # chunks the data sets
                epoch_x, epoch_y = mnist.train.next_batch(batch_size)
                epoch_x = epoch_x.reshape((batch_size, n_chunks, chunk_size))

                _, c = sess.run([optimizer, cost], feed_dict={x: epoch_x, y: epoch_y})
                epoch_loss += c
            print('Epoch', epoch, 'completed out of', hm_epochs, 'loss:', epoch_loss)
            # Till here we are only training the data.

        ## testing
        correct = tf.equal(tf.argmax(prediction, 1), tf.argmax(y, 1))
        accuracy = tf.reduce_mean(tf.cast(correct, 'float'))
        print('Accuracy:',
              accuracy.eval({x: mnist.test.images.reshape((-1, n_chunks, chunk_size)), y: mnist.test.labels}))


train_neural_network(x)

