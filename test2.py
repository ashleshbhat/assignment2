
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

def f1(tuparg,tuparg2):
    if tuparg2[0] in tuparg:
        pos = tuparg.index(tuparg2[0])
        listnew = list(tuparg)
        listnew[pos+1] +=1
        tuparg = tuple(listnew)
    else:
        tuparg +=tuparg2
        tuparg = tuple(tuparg)
       
    return tuparg 

CountMap = tuple(map(lambda word: (word,1), logsplit))
Reduced =(reduce(lambda x,y: f1(x,y), CountMap))
MyWord =  filter(lambda word :isinstance(word,str),Reduced )
MyCount =  filter(lambda word :isinstance(word,int),Reduced )
Mytuple = list(map(lambda word,count : (word,count), MyWord, MyCount))
MySorted = Mytuple.sort(key = lambda tup : tup[1]) 

print(MySorted)  

t2 = time.time()

#print(Reduced)
print (t2-t1)