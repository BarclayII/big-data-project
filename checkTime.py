from __future__ import print_function

import sys
from datetime import datetime
from datetime import timedelta
'''
Requires spark-csv to run
$ pyspark --packages com.databricks:spark-csv_2.11:1.5.0
'''
from pyspark import SparkContext
from pyspark.storagelevel import StorageLevel
from pyspark.sql import HiveContext
from pyspark.sql.types import IntegerType, StructType, StructField
#from pyspark.mllib.linalg import Vectors, SparseVector
from collections import OrderedDict
#import numpy as NP


def init_spark(verbose_logging=False, show_progress=False):
    if not show_progress:
        SparkContext.setSystemProperty('spark.ui.showConsoleProgress', 'false')
    sc = SparkContext()
    sqlContext = HiveContext(sc)
    if verbose_logging:
        sc.setLogLevel(
                'INFO' if isinstance(verbose_logging, bool)
                else verbose_logging
                )
    return sc, sqlContext

def read_hdfs_csv(sqlContext, filename, header='true', sep=','):
    csvreader = (sqlContext
            .read
            .format('com.databricks.spark.csv')
            .options(header=header, inferschema='true', separator=sep)
            )
    return csvreader.load(filename)

def write_hdfs_csv(df, filename, compress=None):
    '''
    Parameters:
        compress: bool
            If True, compress the output to a gzip
    '''
    csvwriter = (
            df.write
            .format('com.databricks.spark.csv')
            .options(header='true')
            )
    if compress:
        csvwriter = csvwriter.options(codec='gzip')
    csvwriter.save(filename)

if __name__ == "__main__":

    sc, sqlC = init_spark()
    data = read_hdfs_csv(sqlC, sys.argv[1])

    #Your code goes here
    def mapper(line):

        key = line[0]
    	df = str(line[1])
        tf = str(line[2])
    	dt = str(line[3])
	tt = str(line[4])

        text_from = df + " " + tf
        text_to = dt + " " + tt
	
        if df != "" and tf !="":
            if dt != "" and tt != "":
                result = "period"
	    elif dt == "" and tt == "":
	        result = "exact"
	    else:
                return (key, ("fieldErr", text_from, text_to))
        elif df == "" and tf == "" and dt != "" and tt != "":
	    result = "end-only"
	else:
	    return (key, ("fieldErr", text_from, text_to))
    
        time_from = None
        time_to = None
        try:
            if result != "end-only":
                if tf == '24:00:00' :
                    tf = '00:00:00'
                    time_from = datetime.strptime(df+" "+tf, "%m/%d/%Y %H:%M:%S")
                    time_from += timedelta(days=1)
                else:
                    time_from = datetime.strptime(df+" "+tf, "%m/%d/%Y %H:%M:%S")
            if result != "exact":
                if tt == '24:00:00' :
                    tt = '00:00:00'
                    time_to = datetime.strptime(dt+" "+tt, "%m/%d/%Y %H:%M:%S")
                    time_to += timedelta(days=1)
                else:
                    time_to = datetime.strptime(dt+" "+tt, "%m/%d/%Y %H:%M:%S")
        except:
            return (key, ("parseErr", text_from, text_to))
        if result == "period":
    	    if time_to < time_from:
    	    	result = "periodErr"
            elif time_from == time_to:
                result = "exact"

        return (key, (result, str(time_from), str(time_to)))

    def counter(line):
        return (line[1][0], 1)

    def reducer(a, b):
	    return a+b

    def formatter(line):
	    return '%s\t%s\t%s\t%s' % (line[0], line[1][0], line[1][1], line[1][2])
	    
    def formatter2(line):
	    return '%s\t%d' % (line[0], line[1])

    result = data.map(mapper)
    result.map(formatter).saveAsTextFile("checkTime.out")
    count = result.map(counter).reduceByKey(reducer)
    count.map(formatter2).saveAsTextFile("checkTimeCount.out")
    
    sc.stop()
