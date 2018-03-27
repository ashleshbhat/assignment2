#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import os
if sys.platform == 'darwin':
    # OS X
    print ("using mac")
elif sys.platform == 'win32':
    # Windows
    sys.path.append("C:/spark-2.2.1-bin-hadoop2.7/python")
    sys.path.append("C:/spark-2.2.1-bin-hadoop2.7/python/lib/py4j-0.10.4-src.zip")
    os.environ['SPARK_HOME'] = "C:\spark-2.2.1-bin-hadoop2.7"
    os.environ['HADOOP_HOME'] = "C:\spark-2.2.1-bin-hadoop2.7"
# elif sys.platform == 'linux' or sys.platform == 'linux2':
    # Linux

import pyspark
from pyspark import SparkContext
from pyspark import SparkConf
import time

t1 = time.time()
logFile = "output100.txt"  
sc = SparkContext("local", "first app")
logData = sc.textFile(logFile).cache()  

logData_count = logData.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)\
             .map(lambda aTuple: (aTuple[1], aTuple[0])).sortByKey()\
             .map(lambda aTuple: (aTuple[0], aTuple[1])).collect()

t2 = time.time()
#logData_count.saveAsTextFile("count")
#numAs = logData.filter(lambda s: 'a' in s).count()
#numBs = logData.filter(lambda s: 'b' in s).count()
#print "Lines with a: %i, lines with b: %i" % (numAs, numBs)
print (logData_count)
print (t2-t1)