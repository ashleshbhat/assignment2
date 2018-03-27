from pyspark.sql import Row
from pyspark import SparkContext
from pyspark import SparkConf
from functools import reduce

l = [('Ankit',25),('Ankit',25),('Jalfraizy',22),('saurabh',20),('bala',20),('bala',20)]
# rdd = sc.p

# list = reduce(lambda a,b: a if (a > b) else b, [47,11,42,102,13])
# print(list)
listFil = list(filter(lambda a,b: (a[0] == b[1]), l))
print(listFil)
fib = [0,1,1,2,3,5,8,13,21,34,55]
result = list(filter(lambda x: x % 2, fib))
print (result)

test = reduce(lambda a,b: (a[0],a[1]+b[1]) if (a[0] == b[0]) else a ,l)
print(test)

my_list = range(16)
print (list(filter(lambda x: x % 3 == 0, my_list)))