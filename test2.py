
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

#function for counting words
def f1(tuparg,tuparg2):
    if tuparg2[0] in tuparg:                        #if the currently reduced tuple is already reduced list of tuple
        pos = tuparg.index(tuparg2[0])              #find the location of the tuple
        listnew = list(tuparg)                      #convert tuple to list for easy modification
        listnew[pos+1] +=1                          #increment the count of the word
        tuparg = tuple(listnew)                     #convert back totuple
    else:                                           #if the currently reduced tuple not it already reduced tuple
        tuparg +=tuparg2                            #add the tuple to the newly reduced list of tuple
    return tuparg                                   #return newly reduced tuple


CountMap = tuple(map(lambda word: (word,1), logsplit))          
Reduced =(reduce(lambda x,y: f1(x,y), CountMap))
Myword =  filter(lambda word :isinstance(word,str),Reduced )
Mycount =  filter(lambda word :isinstance(word,int),Reduced )
Mytuple = list(map(lambda word,count : (word,count), Myword, Mycount))
Mytuple.sort(key = lambda tup : tup[1]) 
 
#Red_map = tuple(reduce(lambda x,y: f2(x,y), Reduced))
#red_sort = sorted(Reduced, key = lambda tup :tup[1])
#Red_sort = sorted(list(Reduced) ,key=lambda pair: pair[1])
print(Mytuple)  

t2 = time.time()

#print(Reduced)
print (t2-t1)