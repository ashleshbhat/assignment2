#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ==================================
# map reduce of python to count words
# ==================================
# importing libraries
import pyspark
from pyspark import SparkContext
from pyspark import SparkConf
import time
from functools import reduce
import re
from StopWords import clean_stopwords
# ==================================

logFilename = "output.txt"  
logFile = open(logFilename)
logData = logFile.read()
logData = clean_stopwords(logData)
ListOnlyAlpha = re.compile('[a-zA-Z]+').findall(logData)

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

t1 = time.time() # start time counter
print ("Program started " + str(t1))
CountMap = tuple(map(lambda word: (word,1), ListOnlyAlpha))
Reduced =(reduce(lambda x,y: f1(x,y), CountMap))
MyWord =  filter(lambda word :isinstance(word,str),Reduced )
MyCount =  filter(lambda word :isinstance(word,int),Reduced )
Mytuple = list(map(lambda word,count : (word,count), MyWord, MyCount))
Mytuple.sort(key = lambda tup : tup[1]) 

print(Mytuple)

 

t2 = time.time()

#print(Reduced)
print (t2-t1)