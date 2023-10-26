from pyspark import SparkConf, SparkContext

#Spark Conf is for configuring the sparkcontext that whether we want to
#run the program on the cluster or on the local machine

#Spark Context is for creating the RDD


import collections
#collections package is for sorting the result at the end





#Here we set master as local as we want to run this on local machine
#It is a single process with single thread for uni/multi processor system

#We set the app name to be the name of Spark UI
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")


sc = SparkContext(conf = conf)

#Loading the u.data and creating a RDD which is called lines.
#Every row in u.data dataset will be the element of lines RDD
#So lines RDD will contain 5 values
lines = sc.textFile("u.data")

#Then we split up the lines RDD in which each element was the row of
#u.data. We splitted it up with whitespace and taken the second element
#from each RDD values that is rating in our case
ratings = lines.map(lambda x: x.split()[2])

#Next we count the total no. of ratings for each unique ratings

result = ratings.countByValue()

#Then we will apply collections package on the python object that we got
#at previous example
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
