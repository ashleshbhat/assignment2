# Big Data Homework 2
by Tobias Joschko and Ashlesh Bhat

For the Homework 2 we created 3 different programs:
- webReader.py which can read a defined number of web pages to create large enough test data sets
- mapReducePython.py which is the sequentiell python version of map reduce in python together with the HW1 wordcounter for time comparison
- SparkWordCounter.py: Spark map/reduce program for word counting

## webReader.py

Takes a search result from www.oldbaileyonline.org/ and extracts the text from all the links in the search results.
The variable num_pages defines how many search results pages shall be screened.
We created the following output files with increasing lengths:
- Output1.txt
- Output10.txt
- Output30.txt
- Output100.txt

## mapReducePython.py

To compare the runtime between the HW1 program and the sequentiell python map reduce we used the "Output30.txt" file for both programs. The results of both programs are the same but we can see signifiant time differences of almost a 10fold for the HW1 counter:

### Map Reduce Python
('took', 815), ('smith', 947), ('man', 965), ('time', 1145), ('mr', 1216), ('john', 1258), ('house', 1265), ('came', 1280), ('went', 1460), ('prisoner', 1597), ('said', 2148)

time used by map reduce:  73.68411421775818

### HW1 Counter
said 2148
prisoner 1597
went 1460
came 1280
house 1265
john 1258
mr 1216
time 1145
man 965
smith 947
took 815

Dict processing time: 635.646905


## SparkWordCounter.py
The usage of Sparks MapReduce offers an even more significant time reduction.
For the same text file used in the example about ("Output30.txt") the following time is needed:
- Countig time needed by Spark:  5.12509894371

For the text file "Output100.txt" which should be roughly 3 times the size of "Output30.txt" just roughly double the time was needed:
- Countig time needed by Spark: 9.65892505646

## Apache Spark

Spark is a fast and general cluster computing system for Big Data. It provides
high-level APIs in Scala, Java, Python, and R, and an optimized engine that
supports general computation graphs for data analysis. It also supports a
rich set of higher-level tools including Spark SQL for SQL and DataFrames,
MLlib for machine learning, GraphX for graph processing,
and Spark Streaming for stream processing.

<http://spark.apache.org/>


## Online Documentation

You can find the latest Spark documentation, including a programming
guide, on the [project web page](http://spark.apache.org/documentation.html).
This README file only contains basic setup instructions.



