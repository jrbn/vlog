import os
import subprocess
import sys
import argparse
from collections import defaultdict
from random import shuffle
from subprocess import check_output, STDOUT, TimeoutExpired


def parse_args():
    parser = argparse.ArgumentParser(description="Run Query generation")
    parser.add_argument('--rules' , type=str, required=True, help = 'Path to the rules file')
    parser.add_argument('--db', type=str, help = 'Path to the directory where results of rules/predicate are stored')
    parser.add_argument('--conf', type=str, required = True, help = 'Path to the configuration file')
    parser.add_argument('--nq', type=int, help = "Number of queries to be executed of each type per predicate", default = 30)
    parser.add_argument('--timeout', type=int, help = "Number of seconds to wait for long running vlog process", default = 25)
    parser.add_argument('--out', type=str, help = 'Name of the query output file')

    return parser.parse_args()


outFile = ""

def generateQueries(rule, arity, resultRecords):

    queries = []
    # Generic query that results in all the possible records
    # Example : ?RP0(A,B)
    query = rule + "("
    for i in range(arity):
        query += chr(i+65)
        if (i != arity-1):
            query += ","
    query += ")"
    queries.append(query)

    # Queries by replacing each variable by a constant
    # We use variable for i and constants for other columns from result records
    if arity > 1:
        for record in resultRecords:
            columns = record.split()
            for a in range(arity):
                query = rule + "("
                for j, column in enumerate(columns):
                    if (a == j):
                        query += chr(i+65)
                    else:
                        query += column

                    if (j != len(columns) -1):
                        query += ","
                query += ")"

                queries.append(query)

    # Boolean queries
    for record in resultRecords:
        columns = record.split()
        for a in range(arity):
            query = rule + "("
            for j, column in enumerate(columns):
                query += column
                if (j != len(columns) -1):
                    query += ","
            query += ")"
            queries.append(query)

    shuffle(queries)
    data = "Query Type QSQR MagicSets"
    iterations = 1
    for q in queries:
        iterations += 1
        if iterations == 10:
            break
        timeoutQSQR = False
        try:
            outQSQR = check_output(['../vlog', 'queryLiteral' ,'-e', args.conf, '--rules', rulesFile, '--reasoningAlgo', 'qsqr', '-l', 'info', '-q', q], stderr=STDOUT, timeout=ARG_TIMEOUT)
        except TimeoutExpired:
            outQSQR = "Runtime = " + str(ARG_TIMEOUT) + "000 milliseconds. #rows = 0\\n"
            timeoutQSQR = True

        strQSQR = str(outQSQR)
        index = strQSQR.find("Runtime =")
        timeQSQR = strQSQR[index+10:strQSQR.find("milliseconds", index)-1]

        #TODO: Find # rows in the output and get the number of records in the result.
        index = strQSQR.find("#rows =")
        numResultsQSQR = strQSQR[index+8:strQSQR.find("\\n", index)]

        timeoutMagic = False
        try:
            outMagic = check_output(['../vlog', 'queryLiteral' ,'-e', args.conf, '--rules', rulesFile, '--reasoningAlgo', 'magic', '-l', 'info', '-q', q], stderr=STDOUT, timeout=ARG_TIMEOUT)
        except TimeoutExpired:
            outMagic = "Runtime = " + str(ARG_TIMEOUT) + "000 milliseconds. #rows = 0\\n"
            timeoutMagic = True

        strMagic = str(outMagic)
        index = strMagic.find("Runtime =")
        timeMagic = strMagic[index+10:strMagic.find("milliseconds", index)-1]

        index = strMagic.find("#rows =")
        numResultsMagic = strMagic[index+8:strMagic.find("\\n",index)]

        if not timeoutQSQR and not timeoutMagic:
            if (numResultsQSQR != numResultsMagic):
                print (numResultsMagic , " : " , numResultsQSQR, "-")

        record = str(q)  + " " + str(timeQSQR) + " " + str(timeMagic)

        print (record)
        data += record

    with open(outFile + ".queries", 'a') as fout:
        fout.write(data)



'''
Takes rule file and rule names array as the input
For every rule checks if we got any resul
'''
def parseRulesFile(rulesFile):
    exploredRules = set()
    with open(rulesFile, 'r') as fin:
        lines = fin.readlines()
        for line in lines:
            head = line.split(':')[0]
            body = line.split('-')[1]
            rule = head.split('(')[0]

            query = line[line.find("TE"):]
            query = query.strip()
            timeoutQSQR = False
            try:
                outQSQR = check_output(['../vlog', 'queryLiteral' ,'-e', args.conf, '--rules', rulesFile, '--reasoningAlgo', 'qsqr', '-l', 'info', '-q', query], stderr=STDOUT, timeout=ARG_TIMEOUT)
            except TimeoutExpired:
                outQSQR = "Runtime = " + str(ARG_TIMEOUT) + "000 milliseconds. #rows = 0\\n"
                timeoutQSQR = True

            strQSQR = str(outQSQR)
            records = strQSQR.split('\\n')

            resultStatRecord = records[-4]
            numberOfRows = int(resultStatRecord[resultStatRecord.find("#rows = ") + 8:])
            if numberOfRows == 0:
                # apply recursive rule exploration strategy
                print (rule , " predicate is not in the database")
                x = 4
            else:
                resultRecords = records[3:len(records)-5]
                columns = records[3].split()
                arity = len(columns)
                print ("generating queries for ", rule)
                generateQueries(rule, arity, resultRecords)
            # Execute the body as a query using vlog and gather results
            # If 0 rows are generated, then it means that the predicate of this rule is not present in the database (directly)
            # and we need to recursively apply the rule. e.g. RP0 : <A, Colleague of, B> is not present in the database.
            # But we can find RP0: RP29, RP30 in the rules file and RP29 and RP30 have the predicates that are present in the database.

resultFiles = []
args = parse_args()
ARG_TIMEOUT = args.timeout
ARG_NQ = args.nq
rulesFile = args.rules
outFile = args.out

parseRulesFile(rulesFile)

