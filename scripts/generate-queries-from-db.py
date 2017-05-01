import os
import subprocess
import sys
import argparse
from collections import defaultdict
from random import shuffle

def parse_args():
    parser = argparse.ArgumentParser(description="Run Query generation")
    parser.add_argument('--rules' , type=str, help = 'Path to the rules file')
    parser.add_argument('--db', type=str, help = 'Path to the directory where results of rules/predicate are stored')
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


def parseResultFile(name, resultFile):
    results = defaultdict(list)
    arity = 0
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

            # Execute the body as a query using vlog and gather results
            # If 0 rows are generated, then it means that the predicate of this rule is not present in the database (directly)
            # and we need to recursively apply the rule. e.g. RP0 : <A, Colleague of, B> is not present in the database.
            # But we can find RP0: RP29, RP30 in the rules file and RP29 and RP30 have the predicates that are present in the database.

resultFiles = []
rulesFile = args.rules
outFile = args.out

parseRulesFile(rulesFile)

