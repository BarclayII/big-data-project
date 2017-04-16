import re
from pyspark.sql import Row
from pyspark.sql.functions import lit
from util import read_hdfs_csv, init_spark
import datetime
import numpy as np
from dateutil import parser
import pickle

#--packages com.databricks:spark-csv_2.11:1.5.0

#STARTING FROM MONDAY AT MIDNIGHT
#Convert date and time of day to hour of week
def date_to_hour(date,time):

    day_of_week = parser.parse(date).weekday()
    hour = int(time.split(':')[0])

    return(day_of_week *24 + hour)

if __name__ == '__main__':
        
    sc, sqlContext = init_spark(verbose_logging=True)

    rows = read_hdfs_csv(sqlContext, '/user/qg323/rows.csv')

    df = rows.filter('CMPLNT_FR_TM != "" and CMPLNT_FR_DT != ""').select('CMPLNT_FR_DT','CMPLNT_FR_TM') \
            .map(lambda x: (date_to_hour(x[0],x[1]),1)) \
            .reduceByKey(lambda x,y:x+y)

    hour = df.collect()

    pickle.dump(hour, open('hourly_freq.p','wb'))
    
        

