import re
from pyspark.sql import Row
from pyspark.sql.functions import lit
from util import read_hdfs_csv, init_spark
import datetime
import numpy as np
from dateutil import parser
import pickle

#--packages com.databricks:spark-csv_2.11:1.5.0


if __name__ == '__main__':
        
    sc, sqlContext = init_spark(verbose_logging=True)

    rows = read_hdfs_csv(sqlContext, '/user/qg323/rows.csv')


    df = rows.select('LAW_CAT_CD' , 'OFNS_DESC' ) \
            .map(lambda x: ((x['LAW_CAT_CD'],x['OFNS_DESC'] ),1)) \
            .reduceByKey(lambda x,y:x+y)

    crimes = df.collect()
    print(len(crimes))
    print(sorted(crimes, key = lambda x:x[1]))

    pickle.dump(crimes, open('crime_type.p','wb'))

