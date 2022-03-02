from TAR.FTPMining import FTPMining
from .Model import *
import csv
import time
import os
import json
import psutil


def TAR(config):
    datapath = config.Dataset
    sid = set()
    database = []
    with open(datapath, "r") as csvseq:
        reader = csv.reader(csvseq)
        csvseq.seek(0, 0)
        for row in reader:
            database.append(row)
            sid.add(int(row[-1]))

    num_sequence = max(sid) - min(sid) + 1

    name = config.Name
    resultDir = config.ResultsDir
    maxPatternSize = config.MaxPatternSize
    savePatterns = config.SavePatterns
    epsilon = config.Epsilon
    minOverlap = config.MinOverlap
    minsup = config.MinSupport * num_sequence
    minconf = config.MinConfidence

    FTP = FTPMining(database, minsup, minconf, epsilon, minOverlap, maxPatternSize)

    start = time.time()
    FTP.Find1Event()
    FTP.Find2FrequentPatterns()
    FTP.FindKFrequentPatterns()

    end = time.time()

    durationTime = end - start
    process = psutil.Process(os.getpid())
    memoryValue = process.memory_info().rss  # in bytes
    if not os.path.exists(resultDir):
        os.makedirs(resultDir)

    resourceDict = {
        'time': durationTime,
        'memory': memoryValue,
        'relations': FTP.num_patterns}

    with open(os.path.join(resultDir, 'Resources'), 'w') as f:
        json.dump(resourceDict, f)

    if savePatterns:
        for level in FTP.Nodes:
            list_json_obj = []
            for id in FTP.Nodes[level]:
                node = FTP.Nodes[level][id]
                json_object = node.to_dict(FTP.EventTable, num_sequence, FTP.EventInstanceTable)
                if json_object:
                    list_json_obj.append(json_object)

            with open(os.path.join(resultDir, '{}.json'.format(level)), 'w') as outfile:
                json.dump(list_json_obj, outfile)
