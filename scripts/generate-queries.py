import os
import subprocess
import sys
import random
import argparse
import copy
import time
from collections import defaultdict
from random import shuffle
from subprocess import check_output, STDOUT, TimeoutExpired, CalledProcessError

STR_magic_time = "magic time ="
STR_qsqr_time = "qsqr time ="
STR_time = "query runtime ="
#STR_rows = "#rows ="
STR_vector = "Vector:"

def generateQueries(rule, arity, resultRecords):

    countQueries = 0
    # Generic query that results in all the possible records
    # Example : ?RP0(A,B)
    query = rule + "("
    for i in range(arity):
        query += chr(i+65)
        if (i != arity-1):
            query += ","
    query += ")"

    if (len(resultRecords) > 4):
        if (150 in queries):
            queries[150].append(query)
        else:
            queries[150] = [query]
    else:
        queries[100+(len(resultRecords))] = [query]

    countQueries += 1
    # Queries by replacing each variable by a constant
    # We use variable for i and constants for other columns from result records
    if arity > 1:
        for i, key in enumerate(sorted(resultRecords)):
            # i will be the type of query for us

            # TODO: reduce the size of resultRecords[key] table to generate queries faster.
            #maxRecords = min(20, len(resultRecords[key]))
            #sampleRecords = resultRecords[key][:maxRecords]
            sampleRecords = resultRecords[key]

            for record in sampleRecords:
                for a in range(arity):
                    query = rule + "("
                    for j, column in enumerate(record):
                        if (a == j):
                            query += chr(j+65)
                        else:
                            query += column

                        if (j != len(record) -1):
                            query += ","
                    query += ")"

                    # Fix the number of types
                    # If step count is >3, write 50 as the query type
                    if (i > 3):
                        if 50 in queries:
                            queries[50].append(query)
                            countQueries += 1
                        else:
                            queries[50] = [query]
                            countQueries += 1
                    else:
                        if (i+1 in queries):
                            queries[i+1].append(query)
                            countQueries += 1
                        else:
                            queries[i+1] = [query]
                            countQueries += 1

    # Boolean queries
    for i, key in enumerate(sorted(resultRecords)):
        # i will be the type for us
        for record in resultRecords[key]:
            for a in range(arity):
                query = rule + "("
                for j, column in enumerate(record):
                    query += column
                    if (j != len(record) -1):
                        query += ","
                query += ")"
                if (i > 3):
                    if (1050 in queries):
                        queries[1050].append(query)
                        countQueries += 1
                    else:
                        queries[1050] = [query]
                        countQueries += 1
                else:
                    if (1000+i+1 in queries):
                        queries[1000+i+1].append(query)
                        countQueries += 1
                    else:
                        queries[1000+i+1] = [query]
                        countQueries += 1

    return countQueries


def runQueryWithAlgo(q, algo, startString, endString, maxTime):
    timeout = False
    time = 0
    try:
        output = check_output(['../vlog', 'queryLiteral' ,'-e', args.conf, '--rules', rulesFile, '--reasoningAlgo', algo, '-l', 'info', '-q', q], stderr=STDOUT, timeout=maxTime)
    except TimeoutExpired:
        output = "Runtime = " + str(ARG_TIMEOUT) + "000 milliseconds. #rows = 0\\n"
        timeout = True
    except CalledProcessError:
        sys.stderr.write("Exception raised because of the following query:")
        sys.stderr.write(q)
        sys.stderr.write("\n")
        timeout = True

    if timeout:
        time = ARG_TIMEOUT*1000
    else:
        output = str(output)
        index = output.find(startString)
        time = output[index+len(startString)+1:output.find(endString, index)]
    return time

'''
This function takses the queries dictionary has the input.
Query type is the key and list of queries of that type is the value.
'''
def runQueries(queries):
    data = ""
    queryStats = ""
    featureString = ""
    global numQueries
    for queryType in queries.keys():
        print ("Running queries of type : ", queryType)
        uniqueQueries = list(set(queries[queryType]))
        shuffle(uniqueQueries)
        cntQSQRWon = 0
        cntMagicWon = 0
        iterations = 0
        timeoutCount = 0
        for q in uniqueQueries:
            print ("Iteration #", iterations, " : " , q)
            # Here invoke vlog and execute query and get the runtime
            workingTimeout = ARG_TIMEOUT
            timeQsqr = runQueryWithAlgo(q, "qsqr", STR_time, "msec", workingTimeout)
            if (float(timeQsqr) / 1000) + 1 < workingTimeout:
                workingTimeout = (float(timeQsqr)/1000)+1
            timeMagic = runQueryWithAlgo(q, "magic", STR_time, "msec", workingTimeout)
            vector_str = runQueryWithAlgo(q, "onlyMetrics", STR_vector, "\\n", ARG_TIMEOUT)
            vector = vector_str.split(',')

            if timeQsqr == timeMagic: #Means both timed out
                print (" timed out ")
                timeoutCount += 1
                if timeoutCount > 10:
                    break

            numQueries += 1
            if float(timeQsqr) < float(timeMagic):
                winnerAlgorithm = "QSQR"
            else:
                winnerAlgorithm = "MAGIC"

            allFeatures = []
            for v in vector:
                allFeatures.append(v)
            #allFeatures.append(numResults)
            allFeatures.append(winnerAlgorithm)

            if float(timeQsqr) < float(timeMagic):
                cntQSQRWon += 1
            else:
                cntMagicWon += 1

            record = str(q) + " " + str(queryType) + " " + str(timeQsqr) + " " + str(timeMagic) + " " + str(allFeatures)
            queryStats += record + "\n"
            print (record)

            featureRecord = ""
            for i, a in enumerate(allFeatures):
                featureRecord += str(a)
                if (i != len(allFeatures)-1):
                    featureRecord += ","
            featureRecord += "\n"
            featureString += featureRecord

            iterations += 1
            #TODO: generate all the possible queries
            if iterations == ARG_NQ:
                break
        # Here we are out of outer loop (of query types)
        # We have counts of qsqr and magic sets
        data += str(queryType) + " " + str(cntQSQRWon) + " " + str(cntMagicWon) + "\n"


    with open(outFile, 'a') as fout:
        fout.write(data)
    with open(outFile + '.features', 'a') as fout:
        fout.write(featureString)
    with open(outFile + '.query.stats', 'w') as fout:
        fout.write(queryStats)

def blocks(fileObject, size=65536):
    while True:
        b = fileObject.read(size)
        if not b:
            break
        yield b

def isFileTooBig(fileName):
    with open(fileName, "r") as fileObject:
        lineCount = sum(block.count("\n") for block in blocks(fileObject))

    if lineCount > ARG_BIGFILE:
        return True
    return False

def parseResultFile(name, resultFile):
    print (name)
    results = defaultdict(list)
    arity = 0
    with open(resultFile, 'r') as fin:
        lines = fin.readlines()
        # If file is too big, then randomly sample 10K records
        if isFileTooBig(resultFile):
            lines = random.sample(lines, ARG_SAMPLE)

    for line in lines:
        columns = line.split()
        arity = len(columns) - 1
        operands = []
        for i, column in enumerate(columns):
            if i == 0:
                continue
            operands.append(column)

        results[int(columns[0])].append(operands)

    nQueries = generateQueries(name, arity, results)
    print (nQueries , " queries generated.")

'''
Takes rule file and rule names array as the input
For every rule checks if we got any resul
'''
def parseRulesFile(rulesFile, rulesWithResult):
    #print(rulesFile, " : ", rulesWithResult)
    exploredRules = set()
    with open(rulesFile, 'r') as fin:
        lines = fin.readlines()
        for line in lines:
            head = line.split(':')[0]
            body = line.split('-')[1]
            rule = head.split('(')[0]
            if rule in exploredRules:
                continue
            exploredRules.add(rule)
            if rule in rulesWithResult:
                print (head, "=>", body)
                parseResultFile(rule, rulesWithResult[rule])

def parse_args():
    parser = argparse.ArgumentParser(description="Run Query generation")
    parser.add_argument('--rules' , type=str, required = True, help = 'Path to the rules file')
    parser.add_argument('--mat', type=str, required = True, help = 'Path to the materialized directory')
    parser.add_argument('--conf', type=str, required = True, help = 'Path to the configuration file')
    parser.add_argument('--nq', type=int, help = "Number of queries to be executed of each type", default = 30)
    parser.add_argument('--timeout', type=int, help = "Number of seconds to wait for long running vlog process", default = 15)
    parser.add_argument('--sample', type=int, help = "Number of lines to sample from the big materialized files", default = 5000)
    parser.add_argument('--bigfile', type=int, help = "Number of lines file should contain so as to be categorized as a big file", default = 10000)
    parser.add_argument('--out', type=str, help = 'Name of the query output file')

    return parser.parse_args()

args = parse_args()
ARG_TIMEOUT = args.timeout
ARG_SAMPLE = args.sample
ARG_BIGFILE = args.bigfile
ARG_NQ = args.nq
resultFiles = []
rulesFile = args.rules
outFile = args.out
with open(outFile, 'w') as fout:
    fout.write("QueryType QSQR MAGIC\n")

with open(outFile + '.features', 'w') as fout:
    fout.write("")

rulesWithResult = dict()
matDir = args.mat
for root, dirs, files in os.walk(os.path.abspath(matDir)):
    for f in files:
        if not f.startswith('R'):
            continue
        ruleResultFilePath = os.path.join(root, f)
        rulesWithResult[f] = ruleResultFilePath
        resultFiles.append(ruleResultFilePath)

queries = {}
numQueries = 0
start = time.time()
parseRulesFile(rulesFile, rulesWithResult)
runQueries(queries)
end = time.time()
print (numQueries, " queries generated in ", (end-start)/60 , " minutes")
