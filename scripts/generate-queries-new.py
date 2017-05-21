import os
import subprocess
import sys
import time
import argparse
import copy
from collections import defaultdict
from random import shuffle
from subprocess import check_output, STDOUT, TimeoutExpired, CalledProcessError
import random

STR_magic_time = "magic time ="
STR_qsqr_time = "qsqr time ="
STR_vector = "Vector:"

MAX_LEVELS = 3

def parse_args():
    parser = argparse.ArgumentParser(description="Run Query generation")
    parser.add_argument('--rules' , type=str, required=True, help = 'Path to the rules file')
    parser.add_argument('--conf', type=str, required = True, help = 'Path to the configuration file')
    parser.add_argument('--nq', type=int, help = "Number of queries to be executed of each type per predicate", default = 10)
    parser.add_argument('--timeout', type=int, help = "Number of seconds to wait for long running vlog process", default = 10)
    parser.add_argument('--out', type=str, required=True, help = 'Name of the query output file')

    return parser.parse_args()

def generateQueries(rulePredicate, arity, resultRecords, variableMap):
    # variable map will map which columns of result record map to which variables of the predicate for which
    # we are generating queries
    queries = set()
    features = {}
    # Generic query that results in all the possible records
    # Example : ?RP0(A,B)
    query = rulePredicate + "("
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
                query = rulePredicate + "("
                for j, column in enumerate(columns):
                    if (a == j):
                        if j == 0:
                            features_value = [0, 1]
                        else:
                            features_value = [1, 0]
                        query += chr(j+65)
                    else:
                        if variableMap[j] == -1:
                            break
                        # Use the correct constant
                        query += columns[variableMap[j]]

                    if (j != len(columns) -1):
                        query += ","
                query += ")"

                queries.add(query)
                features[query] = features_value

    # Boolean queries
    uselessColumnExists = False
    for v in variableMap:
        if v == -1:
            uselessColumnExists = True

    if uselessColumnExists == False:
        for record in resultRecords:
            columns = record.split()

            # e.g. RP40(A,B) => RP50(A), RP51(X,Y)
            # Then, results of RP50 will give only one column for booleam queries
            # we have to find out which variable of target predicate it maps to
            if (len(columns) != arity):
                break # boolean queries are not possible

            for a in range(arity):
                query = rulePredicate + "("
                for j, column in enumerate(columns):
                    query += columns[variableMap[j]]
                    if (j != len(columns) -1):
                        query += ","
                query += ")"
                queries.add(query)
                features[query] = [1, 1]

    return queries, features




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

def getVariablesFromAtom(atom):
    index = atom.find('(')
    variableString = atom[index:atom.find(')', index+1)]
    variables = variableString.split(',')
    return variables

def getPredicateFromAtom(atom):
    return atom.split('(')[0]

# Try to find variables of var1 into var2
# and maintain indexes if found, -1 otherwise
def getVariableIndexMap(var1, var2):
    result = []
    for v1 in var1:
        try:
            index = var2.index(v1)
        except ValueError:
            index = -1
        result.append(index)
    return result

def getVariableIndexMapForExtensionalPredicate(var, varExtPred):
    varFinal =[None] * len(var)
    index = 0
    for i, v in enumerate(varExtPred):
        if v not in var:
            continue
        varFinal[index] = v
        index += 1
    return getVariableIndexMap(var, varFinal)

def generateHashTable(rulesFile):
    rulesMap = {}
    with open(rulesFile, 'r') as fin:
        lines = fin.readlines()
        for line in lines:
            head = line.split(':')[0]
            body = line.split(':-')[1]
            rulePredicate = getPredicateFromAtom(head)

            variablesHead = getVariablesFromAtom(head)
            arity = len(head.split(','))
            if (arity > 2):
                sys.stderr.write("head : ", head, "\n")

            body = body.strip()
            atoms = get_atoms(body)
            if rulePredicate not in rulesMap:
                rulesMap[rulePredicate] = {}
                rulesMap[rulePredicate]['arity'] = arity
                rulesMap[rulePredicate]['atoms'] = []

            # Maintain index of TE /extensional predicate
            for i, atom in enumerate(atoms):
                atomPredicate = getPredicateFromAtom(atom)
                if atomPredicate == rulePredicate:
                    continue
                if atom in rulesMap[rulePredicate]['atoms']:
                    continue

                if (atom.find("TE") == 0):
                    rulesMap[rulePredicate]['indexOfExtPredicate'] = i
                    variablesAtom = getVariablesFromAtom(atom)
                    variableMap = getVariableIndexMapForExtensionalPredicate(variablesHead, variablesAtom)
                else:
                    variablesAtom = getVariablesFromAtom(atom)
                    variableMap = getVariableIndexMap(variablesHead, variablesAtom)
                if not all (v == -1 for v in variableMap):
                    rulesMap[rulePredicate]['atoms'].append((atom, variableMap))

    return rulesMap

def analyzeQueries(queries, features):
    global numQueryFeatures
    queries = list(set(queries))
    shuffle(queries)
    logQueries = ""
    data = ""
    iterations = 1
    for q in queries:
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

            index = output.find(STR_vector)
            vector_str = output[index+len(STR_vector)+1:output.find("\\n", index)]
            vector = vector_str.split(',')

            if float(timeQsqr) < float(timeMagic):
                winnerAlgorithm = 1 #"QSQR"
            else:
                winnerAlgorithm = 0 #"MagicSets"

            allFeatures = copy.deepcopy(features[q])
            for v in vector:
                allFeatures.append(v)
            allFeatures.append(winnerAlgorithm)

            if len(allFeatures) > 5 + len(features[q]) + 1:
                errstr = q+ " : " + "QSQR = " + timeQsqr+" Magic = "+timeMagic +" features : " + record + "\n"
                sys.stderr.write(errstr)
            record = ""
            for i, a in enumerate(allFeatures):
                record += str(a)
                if (i != len(allFeatures)-1):
                    record += ","
            print(q ," : ", record)
            logQueries += q + "\n"
            record += "\n"
            data += record

    with open(outFile + ".csv", 'a') as fout:
        fout.write(data)
    with open(outFile + ".queries", 'a') as fout:
        fout.write(logQueries)

'''
Function which accepts a query and creates a vlog subprocess to run it.
Parses the output of vlog and returns the array of result records
'''
def runVlog(query):
    timeoutQSQR = False
    try:
        outQSQR = check_output(['../vlog', 'queryLiteral' ,'-e', args.conf, '--rules', rulesFile, '--reasoningAlgo', 'qsqr', '-l', 'info', '-q', query], stderr=STDOUT, timeout=ARG_TIMEOUT)
    except TimeoutExpired:
        outQSQR = "Runtime = " + str(ARG_TIMEOUT) + "000 milliseconds. #rows = 0\\n"
        timeoutQSQR = True

    strQSQR = str(outQSQR)
    records = strQSQR.split('\\n')

    if timeoutQSQR == True:
        return []

    resultStatRecord = records[-4]
    numberOfRows = int(resultStatRecord[resultStatRecord.find("#rows = ") + 8:])
    if numberOfRows != 0:
        resultRecords = records[3:len(records)-5]
        #print ("generating queries for ", rulePredicate)
        # resultRecord contains all tuples from database that matched
        # Pass only 10 pairs to generate query
        maxRecords = min(10, len(resultRecords))
        sampleRecords = resultRecords[:maxRecords]
        return sampleRecords
    return []

def exploreAllRules(rulesMap, recurseLevel, vmTarget, targetPredicate, vmPrev, prevPredicate, vmCur, curPredicate, resultQueries, resultFeatures):

    print ("recursion level : ", recurseLevel)
    if recurseLevel > MAX_LEVELS:
        return
    indexExt = rulesMap[curPredicate]['indexOfExtPredicate']
    workingPredicate = rulesMap[curPredicate]['atoms'][indexExt][0]

    query = workingPredicate
    query = query.strip()

    resultRecords = runVlog(query)
    if (len(resultRecords) != 0):
        print ("Generating queries for ", targetPredicate, " with map : ", str(rulesMap[curPredicate]['atoms'][indexExt][1]) )
        someQueries, featureMap = generateQueries(targetPredicate, rulesMap[targetPredicate]['arity'], resultRecords, rulesMap[curPredicate]['atoms'][indexExt][1])
        for q in someQueries:
            resultQueries.append(q)
        resultFeatures.update(featureMap)
        return

    print ("Working predicate ", workingPredicate , " did not produce any results")
    foundUsefulPredicate = False
    for index, atom in enumerate(rulesMap[curPredicate]['atoms']):
        if index == rulesMap[curPredicate]['indexOfExtPredicate']:
            continue
        atomPredicate = getPredicateFromAtom(atom[0])
        atomMap = atom[1]
        indexExt = rulesMap[atomPredicate]['indexOfExtPredicate']
        workingPredicate = rulesMap[atomPredicate]['atoms'][indexExt][0]
        workingMap = rulesMap[atomPredicate]['atoms'][indexExt][1]

        print ("atom predicate: " , atomPredicate, " atomMap : ", str(atomMap))
        print ("working predicate: " , workingPredicate, " workingMap : ", str(workingMap))
        query = workingPredicate
        query = query.strip()
        resultRecords = runVlog(query)
        if (len(resultRecords) != 0):
            for i,a in enumerate(atomMap):
                if a == -1:
                    workingMap[i] = -1
            print("Generating queries for ", targetPredicate, " with map : ", str(workingMap))
            someQueries, featureMap = generateQueries(targetPredicate, rulesMap[targetPredicate]['arity'], resultRecords, workingMap)
            for q in someQueries:
                resultQueries.append(q)
            resultFeatures.update(featureMap)
            return
    if not foundUsefulPredicate:
        for index, atom in enumerate(rulesMap[curPredicate]['atoms']):
            if index == rulesMap[curPredicate]['indexOfExtPredicate']:
                continue
            workingPredicate = getPredicateFromAtom(atom[0])
            workingMap = atom[1]
            if targetPredicate == prevPredicate:
                vmTarget = workingMap
            prevPredicate = curPredicate
            curPredicate = workingPredicate
            vmPrev = vmCur
            vmCur = workingMap
            newQueries = []
            newFeatureMap = {}
            exploreAllRules(rulesMap, recurseLevel+1, vmTarget, targetPredicate, vmPrev, prevPredicate, vmCur, curPredicate, newQueries, newFeatureMap)
            for q in newQueries:
                resultQueries.append(q)
            resultFeatures.update(newFeatureMap)
    # Return after all recursive calls
    return

resultFiles = []
args = parse_args()
ARG_TIMEOUT = args.timeout
ARG_NQ = args.nq
rulesFile = args.rules
outFile = args.out
numQueryFeatures = 0
with open(outFile + ".csv", 'w') as fout:
    fout.write("")
with open(outFile + ".queries", 'w') as fout:
    fout.write("")
start = time.time()
rulesMap = generateHashTable(rulesFile)
# TODO: Randomly select 100 rules
index = 0
items = list(rulesMap.keys())
random.shuffle(items)
for rulePredicate in items:
    index += 1
    if index > 100:
        break
    print ("Predicate ", rulePredicate , " : ")
    resultQueries = [] # List of lists of queries
    resultFeatures = {} # Map of queries to features
    initMap = [None] * rulesMap[rulePredicate]['arity']
    for i in range(rulesMap[rulePredicate]['arity']):
        initMap[i] = i
    exploreAllRules(rulesMap, 0, initMap, rulePredicate, None, rulePredicate, None, rulePredicate, resultQueries, resultFeatures)
    analyzeQueries(resultQueries, resultFeatures)
end = time.time()
print (numQueryFeatures, " queries generated in ", (end-start)/60 , " minutes")
