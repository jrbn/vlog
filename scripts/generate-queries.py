import os
import subprocess
import sys
import argparse
from collections import defaultdict
from random import shuffle

def parse_args():
    parser = argparse.ArgumentParser(description="Run Query generation")
    parser.add_argument('--rules' , type=str, help = 'Path to the rules file')
    parser.add_argument('--mat', type=str, help = 'Path to the materialized directory')
    parser.add_argument('--out', type=str, help = 'Name of the query output file')

    return parser.parse_args()


outFile = ""

def generateQueries(rule, arity, resultRecords):

    queries = []
    #types = []

    # Generic query that results in all the possible records
    # Example : ?RP0(A,B)
    query = rule + "("
    for i in range(arity):
        query += chr(i+65)
        if (i != arity-1):
            query += ","
    query += ")"
    queries.append((query, 100 + len(resultRecords)))

    # Queries by replacing each variable by a constant
        # We use variable for i and constants for other columns from result records
    for i, key in enumerate(sorted(resultRecords)):
        # i will be the type for us
        for record in resultRecords[key]:
            for a in range(arity):
                query = rule + "("
                for j, column in enumerate(record):
                    if (a == j):
                        query += chr(i+65)
                    else:
                        query += column

                    if (j != len(record) -1):
                        query += ","
                query += ")"

                # Fix the number of types
                # If step count is >3, write 50 as the query type
                if (i > 3):
                    queries.append((query, 50))
                else:
                    queries.append((query, i+1))

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
                queries.append((query, 1000 + i + 1))

    shuffle(queries)
    data = "Query Type QSQR MagicSets"
    iterations = 1
    for q in queries:
        iterations += 1
        if iterations == 10:
            break
        # Here invoke vlog and execute query and get the runtime
        processQSQR = subprocess.Popen(['../vlog', 'queryLiteral' ,'-e','edb.conf', '--rules', 'dlog/LUBM1_LE.dlog', '--reasoningAlgo', 'qsqr', '-l', 'info', '-q', q[0]], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        outQSQR, errQSQR = processQSQR.communicate()

        strQSQR = str(errQSQR)
        index = strQSQR.find("Runtime")
        timeQSQR = strQSQR[index+10:strQSQR.find("milliseconds")-1]

        processMagic = subprocess.Popen(['../vlog', 'queryLiteral' ,'-e','edb.conf', '--rules', 'dlog/LUBM1_LE.dlog', '--reasoningAlgo', 'magic', '-l', 'info', '-q', q[0]], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        outMagic, errMagic = processMagic.communicate()

        strMagic = str(errMagic)
        index = strMagic.find("Runtime")
        timeMagic = strMagic[index+10:strMagic.find("milliseconds")-1]

        record = str(q[0]) + " " + str(q[1]) + " " + str(timeQSQR) + " " + str(timeMagic)

        print (record)
        data += record

    with open(outFile + ".queries", 'a') as fout:
        fout.write(data)


def blocks(fileObject, size=65536):
    while True:
        b = fileObject.read(size)
        if not b:
            break
        yield b


def isFileTooBig(fileName):
    with open(fileName, "r") as fileObject:
        lineCount = sum(block.count("\n") for block in blocks(fileObject))

    if lineCount > 1000000:
        return True
    return False

def parseResultFile(name, resultFile):
    print (name)
    results = defaultdict(list)
    arity = 0
    if isFileTooBig(resultFile):
        print (resultFile, " is too big")
        return
    with open(resultFile, 'r') as fin:
        lines = fin.readlines()
        for line in lines:
            columns = line.split()
            arity = len(columns) - 1
            operands = []
            for i, column in enumerate(columns):
                if i == 0:
                    continue
                operands.append(column)

            results[int(columns[0])].append(operands)

    generateQueries(name, arity, results)
    #print (len(results))

'''
Takes rule file and rule names array as the input
For every rule checks if we got any resul
'''
def parseRulesFile(rulesFile, rulesWithResult):
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
                #print (head, "=>", body)
                parseResultFile(rule, rulesWithResult[rule])

def main(args):

    resultFiles = []
    rulesFile = args.rules
    global outFile
    outFile = args.out
    rulesWithResult = dict()
    matDir = args.mat
    for root, dirs, files in os.walk(os.path.abspath(matDir)):
        for f in files:
            if not f.startswith('R'):
                continue
            ruleResultFilePath = os.path.join(root, f)
            rulesWithResult[f] = ruleResultFilePath
            resultFiles.append(ruleResultFilePath)
            #print (f)

    parseRulesFile(rulesFile, rulesWithResult)

if __name__ == "__main__":
    args = parse_args()
    main(args)
