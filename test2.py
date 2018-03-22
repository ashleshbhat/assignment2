
import pyspark
from pyspark import SparkContext
from pyspark import SparkConf
import time
from functools import reduce
import re

t1 = time.time()
logFilename = "output.txt"  
logFile = open(logFilename)
logData = logFile.read().replace('\n','').replace('-', ' ')
logsplit = logData.split()

def f1(listarg,listarg2):
    if listarg2[0] in listarg:
        pos = listarg.index(listarg2[0])
        listnew = list(listarg)
        listnew[pos+1] +=1
        listarg = tuple(listnew)
    else:
        listarg +=listarg2
       
    return listarg 

CountMap = tuple(map(lambda word: (word,1), logsplit))
Reduced =tuple(reduce(lambda x,y: f1(x,y), CountMap))

print(Reduced)  

t2 = time.time()

#print(Reduced)
print (t2-t1)