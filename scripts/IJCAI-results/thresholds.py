import argparse
import copy
import os
from subprocess import check_output, STDOUT
from sklearn.linear_model import LogisticRegression
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

    if (len(yActual) != len(yPredicted)):
        print("FATAL", len(yActual) , " ! = " , len(yPredicted))
        return 1,1,1,1
    for i in range(len(yPredicted)):
        if yActual[i] == yPredicted[i]:
            TP += 1
            TN += 1
        else:
            FN += 1
            FP += 1
    return float(TP), float(FP), float(TN), float(FN)

def train_and_eval(train_file, test_file, columns, targetColumn):
    # Use different files for training and test features
    X_train = pd.read_csv(train_file, names=COLUMNS, skipinitialspace=True, usecols=columns, engine="python")
    Y_train = pd.read_csv(train_file, names=COLUMNS, skipinitialspace=True, usecols=[targetColumn], engine="python")
    X_test = pd.read_csv(test_file, names=COLUMNS, skipinitialspace=True, usecols=columns, engine="python")
    Y_test = pd.read_csv(test_file, names=COLUMNS, skipinitialspace=True, usecols=[targetColumn], engine="python")

    Y_train = np.ravel(Y_train)
    Y_test = np.ravel(Y_test)

    std_clf = make_pipeline(MinMaxScaler(), LogisticRegression())
    #std_clf = make_pipeline(StandardScaler(), LogisticRegression())
    #std_clf = make_pipeline(RobustScaler(), LogisticRegression())
    std_clf.fit(X_train, Y_train)

    startTime = datetime.now()
    pred_test = std_clf.predict(X_test)
    endTime = datetime.now()
    print ("time to predict : ", endTime-startTime)
    #print ("Cross validation score : ", cross_val_score(LogisticRegression(), X, Y).mean())
    return pred_test, metrics.accuracy_score(Y_test, pred_test)

def checkAccuracy(fileName, column, threshold):
    yPredicted = []
    yExpected = []
    with open(fileName, 'r') as fin:
        for line in fin.readlines():
            columns = line.split(',')
            if (float(columns[column]) > threshold):
                yPredicted.append(0)
            else:
                yPredicted.append(1)
            # Last column is always the result column
            yExpected.append(float(columns[-1]))

    TP, FP, TN, FN = perf_measures(list(yExpected), list(yPredicted))
    precision = TP / (TP + FP)
    recall = TP / (TP + FN)
    f1score = 2*precision * recall / (precision + recall)
    #print (metrics.accuracy_score(yTest, predicted))
    #print ("Precision = ", precision)
    #print("Recall = ", recall)
    print("Accuracy = ", f1score, " for feature ", COLUMNS[column])
    return f1score

def findThreshold(fileName, column):
    xTemp = []
    Y = []
    with open(fileName, 'r') as fin:
        for line in fin.readlines():
            # split into columns and calculate mean, min, max
            columns = line.split(',')
            xTemp.append(float(columns[column]))
            # Last column is always the result column
            Y.append(float(columns[-1]))
    X = np.array(xTemp, dtype=np.float64)
    avgX = np.mean(X)
    minX = np.min(X)
    maxX = np.max(X)

    #if (column == 0):
        # Min-Max Normalization
    #    for i,x in enumerate(X):
    #        X[i] = float(x - avgX) / float(maxX - minX)

    #minX = np.min(X)
    #maxX = np.max(X)
    #print("Feature : ", COLUMNS[column], " => Min = ", minX , " Max = ", maxX)
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
        #if (i % 10000 == 0):
        #    print (i, ":")
        for x,y in zip(X,Y):
            # y value = 0 is for magic sets
            if (x > i and y == 0):
                hits += 1
            elif (x < i and y == 1):
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

predictions, accuracy = train_and_eval(train, test, columns, nFeatures)
print ("Overall accuracy = ", accuracy)

print ("Individual feature wise accuracy: ")
for i in range(6):
    thr, acc = findThreshold(train, i)
    print("Feature : ", COLUMNS[i], " => ", thr , " (", acc , ")" )
    checkAccuracy(test, i, thr)


#test_normalized = normalizeColumn(test, 0)

