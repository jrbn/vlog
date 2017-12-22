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
from datetime import datetime
import pandas as pd
import numpy as np

def parse_args():
    parser = argparse.ArgumentParser(description = "Ablation Study")
    parser.add_argument('--train_data', type=str, required=True, help='Training data csv file')
    parser.add_argument('--test_data', type=str, required=True, help='Test data csv file')
    return parser.parse_args()

COLUMNS = ["cost", "estimate", "countRules", "countUniqueRules", "countQueries", "countIDBPredicates", "algorithm"]

def train_and_eval(train_file, test_file, columns, targetColumn):

    # Split training features into training and test
    #X = pd.read_csv(train_file, names=COLUMNS, skipinitialspace=True, usecols=columns, engine="python")
    #Y = pd.read_csv(train_file, names=COLUMNS, skipinitialspace=True, usecols=[5], engine="python")
    #X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.33, random_state=42)
    #Y = np.ravel(Y)

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

predictionsFile = os.path.splitext(test)[0] + "-predictions.log"
predString = ""
for p in predictions:
    predString += str(float(p)) + "\n"
with open(predictionsFile, 'w') as fout:
    fout.write(predString)


histogramData = ""

for i in range(0,nFeatures+1):
    columns = []
    for j in range(0,nFeatures):
        if (i != j):
            columns.append(j)
    predictions, accuracy = train_and_eval(train, test, columns, nFeatures)
    histogramData += "F"+ str(i+1)  + str(round(accuracy, 3)) + "\n"
    print ("accuracy removing feature (", i, ") = " , accuracy)

ablationFileName = os.path.splitext(train)[0] + "-ablation-result.csv"
with open(ablationFileName, 'w') as fout:
    fout.write("Feature Accuracy\n")
    fout.write(histogramData)
