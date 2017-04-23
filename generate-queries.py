import os
import sys
import argparse
from collections import defaultdict

def parse_args():
    parser = argparse.ArgumentParser(description="Run Query generation")
    parser.add_argument('--rules' , type=str, help = 'Path to the rules file')
    parser.add_argument('--mat', type=str, help = 'Path to the materialized directory')

    return parser.parse_args()


'''
class Rule

Rule name
arity (1,2,...)
predicate
'''

def generateQueries(rule, arity, resultRecords):

    queries = []
    types = []

    # Generic query that results in all the possible records
    # Example : ?RP0(A,B)
    query = "?" + rule + "("
    for i in range(arity-1):
        query += chr(i+65)
        if (i != arity-1):
            query += ","
    query += ")"
    queries.append(query)
    types.append(100 + len(resultRecords))

    # Queries by replacing each variable by a constant
        # We use variable for i and constants for other columns from result records
    for i, key in enumerate(sorted(resultRecords)):
        # i will be the type for us
        for record in resultRecords[key]:
            for a in range(arity):
                query = "?" + rule + "("
                for j, column in enumerate(record):
                    if (a == j):
                        query += chr(i+65)
                    else:
                        query += column
                    
                    if (j != len(record) -1):
                        query += ","
                query += ")"
                queries.append(query)
                types.append(i+1)

    # Boolean queries 
    for i, key in enumerate(sorted(resultRecords)):
        # i will be the type for us
        for record in resultRecords[key]:
            for a in range(arity):
                query = "?" + rule + "("
                for j, column in enumerate(record):
                    query += column
                    if (j != len(record) -1):
                        query += ","
                query += ")"
                queries.append(query)
                types.append(1000 + i + 1)

    data = ""
    for q,t in zip(queries, types):
        data += q + " " + str(t) + "\n"

    with open(rule + ".queries", 'w') as fout:
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
    print (len(results))

'''
Takes rule file and rule names array as the input
For every rule checks if we got any resul
'''
def parseRulesFile(rulesFile, rulesWithResult):
    with open(rulesFile, 'r') as fin:
        lines = fin.readlines()
        for line in lines:
            head = line.split(':')[0]
            body = line.split('-')[1]
            name = head.split('(')[0]
            if name in rulesWithResult:
                print (head, "=>", body)
                parseResultFile(name, rulesWithResult[name])

def main(args):

    resultFiles = []
    rulesFile = args.rules
    rulesWithResult = dict()
    matDir = args.mat
    for root, dirs, files in os.walk(os.path.abspath(matDir)):
        for f in files:
            if not f.startswith('R'):
                continue
            ruleResultFilePath = os.path.join(root, f)
            rulesWithResult[f] = ruleResultFilePath
            resultFiles.append(ruleResultFilePath)
            print (f)

    parseRulesFile(rulesFile, rulesWithResult)

if __name__ == "__main__":
    args = parse_args()
    main(args)
