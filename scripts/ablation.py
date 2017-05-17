import argparse
import copy
import os
from subprocess import check_output, STDOUT, TimeoutExpired, CalledProcessError
from patsy import dmatrices
from sklearn.linear_model import LogisticRegression
from sklearn import metrics
import pandas as pd
import numpy as np

def parse_args():
    parser = argparse.ArgumentParser(description = "Ablation Study")
    parser.add_argument('--train_file', type=str, required=True, help='Training data csv file')
    parser.add_argument('--test_file', type=str, required=True, help='Test data csv file')
    return parser.parse_args()

COLUMNS = ["subjectBound", "objectBound", "numberOfResults", "costOfComputing", "numberOfRules", "numberOfQueries", "numberOfUniqueRules", "algorithm"]

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

def train_and_eval(train_file, test_file, i, n):
    FEATURES = copy.deepcopy(COLUMNS)
    del(FEATURES[i])
    df_train = pd.read_csv(
      train_file,
      names=FEATURES,
      skipinitialspace=True,
      engine="python")
    df_test = pd.read_csv(
      test_file,
      names=FEATURES,
      skipinitialspace=True,
      engine="python")

    # remove NaN elements
    df_train = df_train.dropna(how='any', axis=0)
    df_test = df_test.dropna(how='any', axis=0)

    i = 0;
    dmatString = FEATURES[n-2] + ' ~'
    while i < n-2:
        dmatString += FEATURES[i]
        if i < n-3:
            dmatString += '+'
        i += 1

    #y,X = dmatrices ('algorithm ~ subjectBound + objectBound + numberOfResults + \
    #numberOfRules + numberOfQueries + numberOfUniqueRules', df_train, return_type = "dataframe")
    y, X = dmatrices(dmatString, df_train, return_type = "dataframe")
    y = np.ravel(y)
    model = LogisticRegression()
    model = model.fit(X, y)

    #yTest, xTest = dmatrices ('algorithm ~ subjectBound + objectBound + numberOfResults + \
    #numberOfRules + numberOfQueries + numberOfUniqueRules', df_test, return_type = "dataframe")
    yTest, xTest = dmatrices(dmatString, df_test, return_type = "dataframe")
    # check the accuracy on the training set

    predicted = model.predict(xTest)

    TP, FP, TN, FN = perf_measures(list(yTest.values), list(predicted))

    #print ("TP: ", TP)
    #print ("TN: ", TN)
    #print ("FP: ", FP)
    #print ("FN: ", FN)
    precision = TP / (TP + FP)
    recall = TP / (TP + FN)
    f1score = 2*precision * recall / (precision + recall)
    #print (metrics.accuracy_score(yTest, predicted))
    print ("Precision = ", precision)
    print("Recall = ", recall)
    print("Accuracy = ", f1score)
    return f1score

def generate_feature_files(train, phase):
    with open(train, 'r') as fin:
        lines = fin.readlines()
        n = len(lines[0].split(','))
        data = [""] * (n-1)
        for line in lines:
            columns = line.split(',')
            i = 0
            while i < n-1:
                # Make a deep copy of columns
                features = copy.deepcopy(columns)
                # Delete the ith column
                del(features[i])
                data[i] += ",". join(features)
                i += 1
        print("Finished making features")
        i = 0
        while i < n-1:
            print ("Writing file for feature", str(i+1))
            with open('feature-'+ phase + str(i+1) + '.csv', 'w') as fout:
                fout.write(data[i])
            i += 1
    return n

args = parse_args()
train = args.train_file
test = args.test_file
n = generate_feature_files(train, "train")
generate_feature_files(test, "test")

# Run linear model
i = 0
histogramData= ""
while i < n-1:
    train = 'feature-'+ 'train' + str(i+1) + '.csv'
    test = 'feature-' + 'test' + str(i+1) + '.csv'

    accuracy = train_and_eval(train, test, i, n)
    print ("#### without feature  (", i+1, ")" , COLUMNS[i], ": accuracy = ", accuracy )
    histogramData += COLUMNS[i] + " " + str(accuracy) + "\n"
    # Parse output and get the accuracy
    i += 1

with open('ablation-result.csv', 'w') as fout:
    fout.write("Feature Accuracy\n")
    fout.write(histogramData)

# Clean up
i = 0
while i < n-1:
    os.remove('feature-'+ 'train' + str(i+1) + '.csv')
    os.remove('feature-'+ 'test' + str(i+1) + '.csv')
    i += 1
