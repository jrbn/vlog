# Copyright 2016 The TensorFlow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
"""Example code for TensorFlow Wide & Deep Tutorial using TF.Learn API."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tempfile
from six.moves import urllib

import pandas as pd
import tensorflow as tf
import sys
import numpy as np

flags = tf.app.flags
FLAGS = flags.FLAGS

flags.DEFINE_string("model_dir", "", "Base directory for output models.")
flags.DEFINE_string("model_type", "wide_n_deep",
                    "Valid model types: {'wide', 'deep', 'wide_n_deep'}.")
flags.DEFINE_integer("train_steps", 200, "Number of training steps.")
flags.DEFINE_string(
    "train_data",
    "",
    "Path to the training data.")
flags.DEFINE_string(
    "test_data",
    "",
    "Path to the test data.")

COLUMNS = ["subjectBound", "objectBound", "numberOfResults", "costOfComputing", "numberOfRules", "numberOfQueries", "numberOfUniqueRules", "algorithm"]
LABEL_COLUMN = "label"

# Categorical columns are the ones that have values from the finite set.
CATEGORICAL_COLUMNS = ["subjectBound", "objectBound", "algorithm"]

# Continuous columns are the ones that have any numerical value in continuous range
CONTINUOUS_COLUMNS = ["numberOfResults", "costOfComputing", "numberOfRules", "numberOfQueries", "numberOfUniqueRules"]

def build_estimator(model_dir):
  """Build an estimator."""
  # Sparse base columns.
  subjectBound = tf.contrib.layers.sparse_column_with_keys(column_name="subjectBound",
                                                     keys=["0", "1"])
  objectBound = tf.contrib.layers.sparse_column_with_keys(column_name="objectBound",
                                                     keys=["0", "1"])
  #isClass = tf.contrib.layers.sparse_column_with_keys(column_name="isClass",
  #                                                   keys=["0", "1"])

  # Continuous base columns.
  numberOfResults = tf.contrib.layers.real_valued_column("numberOfResults")
  numberOfRules = tf.contrib.layers.real_valued_column("numberOfRules")
  numberOfQueries = tf.contrib.layers.real_valued_column("numberOfQueries")
  numberOfUniqueRules = tf.contrib.layers.real_valued_column("numberOfUniqueRules")
  costOfComputing = tf.contrib.layers.real_valued_column("costOfComputing")

  # Wide columns and deep columns.
  wide_columns = [subjectBound, objectBound, numberOfResults, costOfComputing, numberOfQueries, numberOfRules, numberOfUniqueRules]

  if FLAGS.model_type == "wide":
    m = tf.contrib.learn.LinearClassifier(model_dir=model_dir,
                                          feature_columns=wide_columns)
  elif FLAGS.model_type == "deep":
    m = tf.contrib.learn.DNNClassifier(model_dir=model_dir,
                                       feature_columns=deep_columns,
                                       hidden_units=[100, 50])
  else:
    m = tf.contrib.learn.DNNLinearCombinedClassifier(
        model_dir=model_dir,
        linear_feature_columns=wide_columns,
        dnn_feature_columns=deep_columns,
        dnn_hidden_units=[100, 50])
  return m


def input_fn(df):
  """Input builder function."""
  # Creates a dictionary mapping from each continuous feature column name (k) to
  # the values of that column stored in a constant Tensor.
  continuous_cols = {k: tf.constant(df[k].values) for k in CONTINUOUS_COLUMNS}
  # Creates a dictionary mapping from each categorical feature column name (k)
  # to the values of that column stored in a tf.SparseTensor.
  categorical_cols = {k: tf.SparseTensor(
      indices=[[i, 0] for i in range(df[k].size)],
      #values=df[k].values,
      values=df[k].astype(str).values,
      shape=[df[k].size, 1])
                      for k in CATEGORICAL_COLUMNS}
  # Merges the two dictionaries into one.
  feature_cols = dict(continuous_cols)
  feature_cols.update(categorical_cols)
  # Converts the label column into a constant Tensor.
  label = tf.constant(df[LABEL_COLUMN].values)
  # Returns the feature columns and the label.
  return feature_cols, label


def train_and_eval():
  """Train and evaluate the model."""
  if FLAGS.train_data:
    train_file_name = FLAGS.train_data
  else:
    print ("Training file not provided")
    sys.exit()

  if FLAGS.test_data:
    test_file_name = FLAGS.test_data
  else:
    print ("Test file not provided")
    sys.exit()

  df_train = pd.read_csv(
      tf.gfile.Open(train_file_name),
      names=COLUMNS,
      skipinitialspace=True,
      engine="python")
  df_test = pd.read_csv(
      tf.gfile.Open(test_file_name),
      names=COLUMNS,
      skipinitialspace=True,
      skiprows=1,
      engine="python")

  # remove NaN elements
  df_train = df_train.dropna(how='any', axis=0)
  df_test = df_test.dropna(how='any', axis=0)

  df_train[LABEL_COLUMN] = (
      df_train["algorithm"].apply(lambda x: "QSQR" in x)).astype(int)
  df_test[LABEL_COLUMN] = (
      df_test["algorithm"].apply(lambda x: "QSQR" in x)).astype(int)

  model_dir = tempfile.mkdtemp() if not FLAGS.model_dir else FLAGS.model_dir
  print("model directory = %s" % model_dir)

  m = build_estimator(model_dir)
  m.fit(input_fn=lambda: input_fn(df_train), steps=FLAGS.train_steps)
  results = m.evaluate(input_fn=lambda: input_fn(df_test), steps=1)
  for key in sorted(results):
    print("%s: %s" % (key, results[key]))

  print ("Predictions : ", str(results))
  #new_samples = np.array([[6, 3, 4, 1.12, 8, 1, 5, 0]], dtype=int)
  #new_samples = [6, 3, 4, 1.12, 8, 1, 5, "QSQR"]
  #y = m.predict(new_samples)
  #print ("Predictions : ", str(y))

def main(_):
  train_and_eval()


if __name__ == "__main__":
  tf.app.run()
