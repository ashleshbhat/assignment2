
import pyspark
from pyspark import SparkContext
from pyspark import SparkConf
import time

t1 = time.time()
logFilename = "README.md"  
logFile = open(logFilename)
logFile = list(logFile)
logData = list(map(lambda line: line.split(' ') ,logFile))
#logData = (map(lambda line: line.split(' '),logData))
print(logData)
t2 = time.time()

print (t2-t1)