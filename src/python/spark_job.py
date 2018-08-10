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
        returnable[value.lower()] = records[index]
    return returnable
    
    
crimes_raw = ( sc
    .textFile("s3://jejoseph-codeacademy-2/crimes/*")
    .filter(lambda x: col_headers not in x)
    )


mapped = crimes_raw.map(create_recordmap)
# crimes = crimes_raw.map(lambda x: x.split("\t"))


mapped.map(lambda x: x['primary type']).distinct().collect()


mapped.map(lambda x: (x['primary type'],1)).reduceByKey(lambda x, y: x+y).collect()