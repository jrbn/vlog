import argparse
import copy
import os
from subprocess import check_output, STDOUT, TimeoutExpired, CalledProcessError
from sklearn.linear_model import LogisticRegression
from sklearn import metrics
from sklearn.preprocessing import StandardScaler, RobustScaler, MinMaxScaler
from sklearn.decomposition import PCA
from sklearn.naive_bayes import GaussianNB
from sklearn.pipeline import make_pipeline
from sklearn.model_selection import train_test_split, cross_val_score

import pandas as pd
import numpy as np

def parse_args():
    parser = argparse.ArgumentParser(description = "Ablation Study")
    parser.add_argument('--train_data', type=str, required=True, help='Training data csv file')
    parser.add_argument('--test_data', type=str, required=True, help='Test data csv file')
    return parser.parse_args()

COLUMNS = ["cost", "estimate", "countRules", "countUniqueRules", "countQueries", "algorithm"]

def train_and_eval(train_file, test_file, columns):

    # Split training features into training and test
    #X = pd.read_csv(train_file, names=COLUMNS, skipinitialspace=True, usecols=columns, engine="python")
    #Y = pd.read_csv(train_file, names=COLUMNS, skipinitialspace=True, usecols=[5], engine="python")
    #X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.33, random_state=42)
    #Y = np.ravel(Y)

    # Use different files for training and test features
    X_train = pd.read_csv(train_file, names=COLUMNS, skipinitialspace=True, usecols=columns, engine="python")
    Y_train = pd.read_csv(train_file, names=COLUMNS, skipinitialspace=True, usecols=[5], engine="python")
    X_test = pd.read_csv(test_file, names=COLUMNS, skipinitialspace=True, usecols=columns, engine="python")
    Y_test = pd.read_csv(test_file, names=COLUMNS, skipinitialspace=True, usecols=[5], engine="python")

    Y_train = np.ravel(Y_train)
    Y_test = np.ravel(Y_test)

    std_clf = make_pipeline(MinMaxScaler(), LogisticRegression())
    #std_clf = make_pipeline(StandardScaler(), LogisticRegression())
    #std_clf = make_pipeline(RobustScaler(), LogisticRegression())
    std_clf.fit(X_train, Y_train)
    pred_test = std_clf.predict(X_test)
    #print ("Cross validation score : ", cross_val_score(LogisticRegression(), X, Y).mean())
    return metrics.accuracy_score(Y_test, pred_test)


args = parse_args()
train = args.train_data
test = args.test_data

accuracy = train_and_eval(train, test, [0,1,2,3,4])
print ("Overall accuracy = ", accuracy)

accuracy = train_and_eval(train, test, [1,2,3,4])
print ("accuracy removing feature 0 = ", accuracy)
accuracy = train_and_eval(train, test, [0,2,3,4])
print ("accuracy removing feature 1 = ", accuracy)
accuracy = train_and_eval(train, test, [0,1,3,4])
print ("accuracy removing feature 2 = ", accuracy)
accuracy = train_and_eval(train, test, [0,1,2,4])
print ("accuracy removing feature 3 = ", accuracy)
accuracy = train_and_eval(train, test, [0,1,2,3])
print ("accuracy removing feature 4 = ", accuracy)
