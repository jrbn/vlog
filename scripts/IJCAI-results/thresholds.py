import argparse
import copy
import os
from subprocess import check_output, STDOUT
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.preprocessing import StandardScaler, RobustScaler, MinMaxScaler
from sklearn.decomposition import PCA
from sklearn.naive_bayes import GaussianNB
from sklearn.pipeline import make_pipeline
from sklearn.model_selection import train_test_split, cross_val_score
from datetime import datetime
from sklearn import metrics
import pandas as pd
import numpy as np

def parse_args():
    parser = argparse.ArgumentParser(description = "Ablation Study")
    parser.add_argument('--train_data', type=str, required=True, help='Training data csv file')
    parser.add_argument('--test_data', type=str, required=True, help='Test data csv file')
    return parser.parse_args()

COLUMNS = ["cost", "estimate", "countRules", "countUniqueRules", "countQueries", "countIDBPredicates", "algorithm"]

def perf_measures(yActual, yPredicted):
    TP = 0
    FP = 0
    FN = 0
    TN = 0
    magicGuess = 0
    qsqrGuess = 0

    if (len(yActual) != len(yPredicted)):
        print("FATAL", len(yActual) , " ! = " , len(yPredicted))
        return 1,1,1,1
    for i in range(len(yPredicted)):
        if yActual[i] == yPredicted[i]:
            TP += 1
            TN += 1
            if (yActual[i] == 0):
                magicGuess += 1
            else:
                qsqrGuess += 1
        else:
            FN += 1
            FP += 1
    return float(TP), float(FP), float(TN), float(FN), qsqrGuess, magicGuess

def train_and_eval(train_file, test_file, columns, targetColumn, estimator):
    # Use different files for training and test features
    X_train = pd.read_csv(train_file, names=COLUMNS, skipinitialspace=True, usecols=columns, engine="python")
    Y_train = pd.read_csv(train_file, names=COLUMNS, skipinitialspace=True, usecols=[targetColumn], engine="python")
    X_test = pd.read_csv(test_file, names=COLUMNS, skipinitialspace=True, usecols=columns, engine="python")
    Y_test = pd.read_csv(test_file, names=COLUMNS, skipinitialspace=True, usecols=[targetColumn], engine="python")

    Y_train = np.ravel(Y_train)
    Y_test = np.ravel(Y_test)

    #std_clf = make_pipeline(estimator)
    std_clf = make_pipeline(MinMaxScaler(), estimator)
    std_clf.fit(X_train, Y_train)

    startTime = datetime.now()
    pred_test = std_clf.predict(X_test)
    endTime = datetime.now()
    print ("time to predict : ", endTime-startTime)
    #print ("Cross validation score : ", cross_val_score(LogisticRegression(), X, Y).mean())
    #return pred_test, metrics.accuracy_score(Y_test, pred_test)
    TP, FP, TN, FN, qsqrGuess, magicGuess = perf_measures(list(Y_test), list(pred_test))
    nQsqrQueries = list(Y_test).count(1)
    nMagicQueries = list(Y_test).count(0)
    precision = float(TP) / float(TP + FP)
    recall = float(TP) / float(TP + FN)
    #f1score = 2*precision * recall / (precision + recall)
    f1score = precision
    print("Overall Accuracy = ", f1score)
    if (nQsqrQueries != 0):
        print("Relative Accuracy (QSQR) = ", (float(qsqrGuess)/float(nQsqrQueries))*100)
    if (nMagicQueries != 0):
        print("Relative Accuracy (MAGIC) = ", (float(magicGuess)/float(nMagicQueries))*100)
    return pred_test, f1score

def checkAccuracy(fileName, column, threshold):
    yPredicted = []
    yExpected = []
    with open(fileName, 'r') as fin:
        for line in fin.readlines():
            columns = line.split(',')
            if (float(columns[column]) < threshold):
                yPredicted.append(0)
            else:
                yPredicted.append(1)
            # Last column is always the result column
            yExpected.append(float(columns[-1]))

    TP, FP, TN, FN, qsqrGuess, magicGuess = perf_measures(list(yExpected), list(yPredicted))
    nQsqrQueries = yExpected.count(1)
    nMagicQueries = yExpected.count(0)
    precision = TP / (TP + FP)
    recall = TP / (TP + FN)
    f1score = 2*precision * recall / (precision + recall)
    #print (metrics.accuracy_score(yTest, predicted))
    #print ("Precision = ", precision)
    #print("Recall = ", recall)
    print("Accuracy on Test set = ", f1score*100)
    if (nQsqrQueries != 0):
        print("Relative Accuracy (QSQR) = ", (float(qsqrGuess)/float(nQsqrQueries))*100)
    if (nMagicQueries != 0):
        print("Relative Accuracy (MAGIC) = ", (float(magicGuess)/float(nMagicQueries))*100)
    return f1score

def findThreshold(fileName, column):
    X = []
    Y = []
    with open(fileName, 'r') as fin:
        for line in fin.readlines():
            columns = line.split(',')
            X.append(float(columns[column]))
            # Last column is always the result column
            Y.append(float(columns[-1]))

    N = len(X)
    if (N != len(Y)):
        print("X and Y arrays length differ")
        exit(1)

    threshold = -1
    maxAccuracy = 0.0
    sortedX = sorted(X)
    for i in  sortedX:
        hits = 0
        misses = 0
        for x,y in zip(X,Y):
            # y value = 0 is for magic sets
            if (x < i and y == 0):
                hits += 1
            elif (x >= i and y == 1):
                hits += 1
            else:
                misses += 1
        accuracy = (float(hits)/float(N)) * 100
        if (accuracy > maxAccuracy):
            maxAccuracy = accuracy
            threshold = i

    return threshold, maxAccuracy

args = parse_args()
train = args.train_data
test = args.test_data


nFeatures = 0
with open (train, 'r') as fin:
    line = fin.readlines()[0]
    nFeatures = len(line.split(',')) - 1

columns = []
for i in range(0,nFeatures):
    columns.append(i)

predictions, accuracy = train_and_eval(train, test, columns, nFeatures, LogisticRegression())
#print ("Overall accuracy = ", accuracy)

#train_and_eval(train, test, [0], nFeatures, LinearRegression())

print ("Individual feature wise accuracy: ")
for i in range(6):
    thr, acc = findThreshold(train, i)
    print("===================================================================")
    print("Feature : ", COLUMNS[i], " =>  threshold = ", thr , " (Accuracy on Training set = ", acc , ")" )
    checkAccuracy(test, i, thr)
print("===================================================================")
