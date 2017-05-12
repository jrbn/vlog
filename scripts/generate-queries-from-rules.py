import os
import subprocess
import sys
import time
import argparse
from collections import defaultdict
from random import shuffle
from subprocess import check_output, STDOUT, TimeoutExpired, CalledProcessError

STR_magic_time = "magic time ="
STR_qsqr_time = "qsqr time ="
#STR_rows = "#rows ="
STR_vector = "Vector:"
def parse_args():
    parser = argparse.ArgumentParser(description="Run Query generation")
    parser.add_argument('--rules' , type=str, required=True, help = 'Path to the rules file')
    parser.add_argument('--conf', type=str, required = True, help = 'Path to the configuration file')
    parser.add_argument('--nq', type=int, help = "Number of queries to be executed of each type per predicate", default = 10)
    parser.add_argument('--timeout', type=int, help = "Number of seconds to wait for long running vlog process", default = 10)
    parser.add_argument('--out', type=str, required=True, help = 'Name of the query output file')

    return parser.parse_args()

def generateQueries(rule, arity, resultRecords):

    global numQueryFeatures
    queries = set()
    features = {}
    # Generic query that results in all the possible records
    # Example : ?RP0(A,B)
    query = rule + "("
    for i in range(arity):
        query += chr(i+65)
        if (i != arity-1):
            query += ","
    query += ")"
    queries.add(query)
    features[query] = [0, 0]

    # Queries by replacing each variable by a constant
    # We use variable for i and constants for other columns from result records
    if arity > 1:
        for record in resultRecords:
            columns = record.split()
            if len(columns) > arity:
                continue

            for a in range(arity):
                query = rule + "("
                for j, column in enumerate(columns):
                    if (a == j):
                        if j == 0:
                            features_value = [0, 1]
                        else:
                            features_value = [1, 0]
                        query += chr(j+65)
                    else:
                        query += column

                    if (j != len(columns) -1):
                        query += ","
                query += ")"

                queries.add(query)
                features[query] = features_value

    # Boolean queries
    for record in resultRecords:
        columns = record.split()

        # e.g. RP40(A,B) => RP50(X), RP51(X,Y)
        # Then, results of RP50 will give only one column for booleam queries
        if (len(columns) != arity):
            break

        for a in range(arity):
            query = rule + "("
            for j, column in enumerate(columns):
                query += column
                if (j != len(columns) -1):
                    query += ","
            query += ")"
            queries.add(query)
            features[query] = [1, 1]

    queriesList = list(queries)
    shuffle(queriesList)
    data = ""
    iterations = 1
    for q in queriesList:
        iterations += 1
        if iterations == ARG_NQ:
            break

        timeout = False
        try:
            output = check_output(['../vlog', 'queryLiteral' ,'-e', args.conf, '--rules', rulesFile, '--reasoningAlgo', 'both', '-l', 'info', '-q', q], stderr=STDOUT, timeout=ARG_TIMEOUT*2)
        except TimeoutExpired:
            output = "Runtime = " + str(ARG_TIMEOUT) + "000 milliseconds. #rows = 0\\n"
            timeout = True
        except CalledProcessError:
            sys.stderr.write("Exception raised because of the following query:")
            sys.stderr.write(q)
            sys.stderr.write("\n")
            timeout = True

        if timeout == False:

            numQueryFeatures += 1

            output = str(output)
            index = output.find(STR_magic_time)
            timeMagic = output[index+len(STR_magic_time)+1:output.find("\\n", index)]

            index = output.find(STR_qsqr_time)
            timeQsqr = output[index+len(STR_qsqr_time)+1:output.find("\\n", index)]

            #index = output.find(STR_rows)
            #numResults = output[index+len(STR_rows)+1:output.find("\\n", index)]

            index = output.find(STR_vector)
            vector_str = output[index+len(STR_vector)+1:output.find("\\n", index)]
            vector = vector_str.split(',')

            if float(timeQsqr) < float(timeMagic):
                winnerAlgorithm = 1 #"QSQR"
            else:
                winnerAlgorithm = 0 #"MagicSets"

            allFeatures = features[q]
            for v in vector:
                allFeatures.append(v)
            #allFeatures.append(numResults)
            allFeatures.append(winnerAlgorithm)

            record = ""
            if len(allFeatures) > 5 + len(features[q]) + 2:
                errstr = q+ " : " + "QSQR = " + timeQsqr+" Magic = "+timeMagic +" features : " + record + "\n"
                sys.stderr.write(errstr)
            for i, a in enumerate(allFeatures):
                record += str(a)
                if (i != len(allFeatures)-1):
                    record += ","
            record += "\n"
            data += record

    with open(outFile + ".csv", 'a') as fout:
        fout.write(data)



'''
Takes rule file and rule names array as the input
For every rule checks if we got any result
'''
'''
RP361(Y) :- RP361(X0),RP307(X0,Y)
RP27(A,B) :- TE(A,<http://dbpedia.org/ontology/Galaxy/surfaceArea>,B)
RP20(<http://ruliee/contradiction>) :- RP21(X1,<http://www.w3.org/2001/XMLSchema#double>),RP158(X,X1)
RP54(X) :- RP159(X,X1)
'''
def get_atoms(body):
    atoms = []
    startIndex = 0
    while True:
        index = body.find(')', startIndex)
        if index == -1:
            break
        atoms.append(body[startIndex:index+1])
        startIndex = index+2
    return atoms

def parseRulesFile(rulesFile):
    rulesMap = {}
    arityMap = {}
    with open(rulesFile, 'r') as fin:
        lines = fin.readlines()
        for line in lines:
            head = line.split(':')[0]
            body = line.split(':-')[1]
            rule = head.split('(')[0]

            arity = len(head.split(','))
            arityMap[rule] = arity
            if (arity > 2):
                sys.stderr.write("head : ", head, "\n")

            body = body.strip()
            atoms = get_atoms(body)
            if rule in rulesMap:
                for atom in atoms:
                    rulesMap[rule].append(atom)
            else:
                for i, atom in enumerate(atoms):
                    if i == 0:
                        rulesMap[rule] = [atom]
                    else:
                        rulesMap[rule].append(atom)

    # Use rulesMap to go over each rule and its possible implications
    for rule in sorted(rulesMap):
        print("Rule ", rule , ":")
        atomIndex = 0
        while atomIndex < len(rulesMap[rule]):
            atom = rulesMap[rule][atomIndex]
            print("Checking atom ", atom)
            if atom.find("TE") == 0:
                query = atom
                query = query.strip()
            else:
                newRule = atom.split('(')[0]
                query = rulesMap[newRule][0]
                if query.find("TE") != 0:
                    print("Rule ", newRule, "does not derive anything useful: ", rulesMap[newRule])
                else:
                    query = query.strip()

            timeoutQSQR = False
            try:
                outQSQR = check_output(['../vlog', 'queryLiteral' ,'-e', args.conf, '--rules', rulesFile, '--reasoningAlgo', 'qsqr', '-l', 'info', '-q', query], stderr=STDOUT, timeout=ARG_TIMEOUT)
            except TimeoutExpired:
                outQSQR = "Runtime = " + str(ARG_TIMEOUT) + "000 milliseconds. #rows = 0\\n"
                timeoutQSQR = True

            strQSQR = str(outQSQR)
            records = strQSQR.split('\\n')

            if timeoutQSQR == False:
                resultStatRecord = records[-4]
                numberOfRows = int(resultStatRecord[resultStatRecord.find("#rows = ") + 8:])
                if numberOfRows == 0:
                    # apply recursive rule exploration strategy
                    print (rule , " predicate is not in the database")
                    print (atom, " did not produce any result records")
                    atomIndex += 1
                    continue
                else:
                    resultRecords = records[3:len(records)-5]
                    print ("generating queries for ", rule)

                    # resultRecord contains all tuples from database that matched
                    # Pass only 10 pairs to generate query
                    maxRecords = min(10, len(resultRecords))
                    sampleRecords = resultRecords[:maxRecords]
                    generateQueries(rule, arityMap[rule], sampleRecords)
                    break
            else:
                atomIndex += 1

resultFiles = []
args = parse_args()
ARG_TIMEOUT = args.timeout
ARG_NQ = args.nq
rulesFile = args.rules
outFile = args.out
numQueryFeatures = 0
with open(outFile + ".csv", 'w') as fout:
    fout.write("")
start = time.time()
parseRulesFile(rulesFile)
end = time.time()
print (numQueryFeatures, " queries generated in ", (end-start)/60 , " minutes")
