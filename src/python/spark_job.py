from pyspark import SparkContext, SparkConf
import sys
conf = SparkConf().setAppName("MyFirstETL")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

col_headers = 'ID,Case Number,Date,Block,IUCR,Primary Type,Description,Location Description,Arrest,Domestic,Beat,District,Ward,Community Area,FBI Code,Latitude,Longitude'
def create_recordmap(raw):
    returnable = {}
    cols = col_headers.split(",")
    records = raw.split(",")
    
    for index, value in enumerate(cols):
        returnable[value] = records[index]
    return returnable