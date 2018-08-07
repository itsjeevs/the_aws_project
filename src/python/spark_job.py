from pyspark import SparkContext, SparkConf
import sys
conf = SparkConf().setAppName("MyFirstETL")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

