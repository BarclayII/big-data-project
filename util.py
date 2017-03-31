'''
This module requires spark-csv package to run:
https://spark-packages.org/package/databricks/spark-csv

To do so, always add the following option to spark-submit or pyspark:
    --packages com.databricks:spark-csv_2.11:1.5.0
'''
from pyspark import SparkContext
from pyspark.sql import HiveContext


def init_spark(verbose_logging=False, show_progress=False):
    '''
    Any PySpark program should call this routine before actually doing
    anything with pyspark packages by:
    >>> sc, sqlContext = init_spark()

    :param: verbose_logging:
        either bool or any of "DEBUG", "INFO", "WARN" or "ERROR"
    :param: show_progress:
        bool, whether or not to show progress bar for each Spark operation.
    '''
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
    '''
    Reads CSV on HDFS and returns a PySpark DataFrame.
    Requires package spark-csv
    '''
    csvreader = (sqlContext
            .read
            .format('com.databricks.spark.csv')
            .options(header=header, inferschema='true', separator=sep)
            )
    return csvreader.load(filename)

def write_hdfs_csv(df, filename, compress=None):
    '''
    :param: compress
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
