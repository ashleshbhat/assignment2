#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ==================================
#
# sequentiel map reduce of python to count words
#
#  ==================================
# importing libraries
import pyspark
from pyspark import SparkContext
from pyspark import SparkConf
import time
from functools import reduce
import re
from StopWords import clean_stopwords
# ==================================

logFilename = "Output1.txt" #input file  
logFile = open(logFilename)
# logData = clean_stopwords(logData)
# ListOnlyAlpha = re.compile('[a-zA-Z]+').findall(logData)
logData = logFile.read().replace("'"," ").replace(",","")
logSplit = logData.split()


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

t1 = time.time()                                    # start time counter
print ("Program started " + str(t1)) 
print ("tuple convertion...")                                   
CountMap = tuple(map(lambda word: (word,1), logSplit))             #convert every word to tuple of word,number
print ("Counting in progress...") 
Reduced =(reduce(lambda x,y: f1(x,y), CountMap))                        #reduce using the above function to count the number of words
print ("words filtering in progress...") 
MyWord =  filter(lambda word :isinstance(word,str),Reduced )            #filter the list to get all the words in a list
print ("Count filtering progress...") 
MyCount =  filter(lambda word :isinstance(word,int),Reduced )           #filter the list to get all the counts of words
print ("Tuple conversion in progress...") 
Mytuple = list(map(lambda word,count : (word,count), MyWord, MyCount))  #make a tuple of word,count
print ("Sorting in progress...") 
Mytuple.sort(key = lambda tup : tup[1])                                 #sort using the key as the count
t2 = time.time()
print(Mytuple)
print ("Done") 
#print(Reduced)
print ("time used by map reduce: ",t2-t1)

## ---------------------------------------
# Homework 1 word counter
t0_hw1 = time.time()
# create list with word freq
wordFreqList = []
for istr in logSplit:
    wordFreqList.append([istr, logSplit.count(istr)])

# create dictionary with word freq
# copy words and count from freqList to a dictionary
wordFreqDict = {istr[0]:istr[1] for istr in wordFreqList}
# sort dictionary by word count
wordFreqDict = sorted(wordFreqDict.items(), key=lambda t:t[1], reverse=True)
clocktimeHW1 = time.time() - t0_hw1
print ("\nDict processing time: %f\n" %clocktimeHW1)
for pair in wordFreqDict[:20]:
    print (pair[0],pair[1])
