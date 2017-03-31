from pyspark import SparkContext
from pyspark.storagelevel import StorageLevel
from pyspark.sql import HiveContext
from pyspark.sql.types import IntegerType, StructType, StructField
from pyspark.mllib.linalg import Vectors, SparseVector
from collections import OrderedDict
import numpy as NP


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
