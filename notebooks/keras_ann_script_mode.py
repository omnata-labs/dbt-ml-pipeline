import tensorflow as tf
import numpy as np
import os
import argparse
from tensorflow.python.keras.models import Sequential
from tensorflow.python.keras.layers import Dense

INPUT_TENSOR_NAME = "inputs_input" # needs to match the name of the first layer + "_input"

def model_builder(optimizer):    
    classifier = Sequential()
    classifier.add(Dense(units = 6, kernel_initializer = "uniform", activation = "relu", input_dim = 13, name = "inputs"))
    classifier.add(Dense(units = 6, kernel_initializer = "uniform", activation = "relu"))
    classifier.add(Dense(units = 1, kernel_initializer = "uniform", activation = "sigmoid"))
    classifier.compile(optimizer = optimizer, loss = "binary_crossentropy", metrics = ["accuracy"])
    return classifier

def get_input_data(training_dir):    
    X_train = np.load(os.path.join(training_dir, 'train_X.npy'))
    y_train = np.load(os.path.join(training_dir, 'train_Y.npy'))
    
    return {INPUT_TENSOR_NAME: X_train}, y_train

def get_test_data(training_dir):
    X_test = np.load(os.path.join(training_dir, 'test_X.npy'))
    y_test = np.load(os.path.join(training_dir, 'test_Y.npy'))

    return {INPUT_TENSOR_NAME: X_test}, y_test

if __name__ =='__main__':
    parser = argparse.ArgumentParser()

    # hyperparameters sent by the client are passed as command-line arguments to the script.
    parser.add_argument('--epochs', type=int, default=10)
    parser.add_argument('--batch_size', type=int, default=100)
    parser.add_argument('--learning_rate', type=float, default=0.1)

    # input data and model directories
    parser.add_argument('--model-dir', type=str, default=os.environ['SM_MODEL_DIR'])
    parser.add_argument('--train', type=str, default=os.environ.get('SM_CHANNEL_TRAIN'))
    parser.add_argument('--eval', type=str, default=os.environ.get('SM_CHANNEL_EVAL'))

    args, _ = parser.parse_known_args()
    
    X_train, y_train = get_input_data(args.train)
    X_test, y_test = get_test_data(args.eval)
    
    optimizer =  tf.train.AdamOptimizer(learning_rate=args.learning_rate)

    classifier = model_builder(optimizer)
    classifier.summary()
    classifier.fit(X_train, y_train, epochs=args.epochs, verbose=1, batch_size = args.batch_size)
    
    score = classifier.evaluate(X_test, y_test, verbose=1)
    print('Test loss:', score[0])
    print('Test accuracy:', score[1])
    print(f'Saving model file to: {args.model_dir}')
    
    # create a TensorFlow SavedModel for deployment to a SageMaker endpoint with TensorFlow Serving
    tf.contrib.saved_model.save_keras_model(classifier, args.model_dir)
    import os
    output_check = os.listdir(args.model_dir)
    print(output_check)
    #tf.saved_model.save(classifier, args.model_dir)

