import re
from pyspark.sql import Row
from pyspark.sql.functions import lit
from util import read_hdfs_csv, init_spark
import datetime
import numpy as np

#--packages com.databricks:spark-csv_2.11:1.5.0



if __name__ == '__main__':
        
    sc, sqlContext = init_spark(verbose_logging=True)

    rows = read_hdfs_csv(sqlContext, '/user/qg323/rows.csv')

    borrow_names = ['MANHATTAN','QUEENS','STATEN ISLAND','BROOKLYN','BRONX']

    for borrow in borrow_names:

        df = rows.filter('BORO_NM =  "' + borrow+'"')
        df = df.select('CMPLNT_FR_DT').map(lambda x: (x['CMPLNT_FR_DT'],1)).reduceByKey(lambda x,y:x+y)
        freq = np.array([ x[1] for x in df.collect() if x[1] > 2])
        np.save(borrow+'.npy',freq)
        
    #days = rows.select('CMPLNT_FR_DT','BORO_NM').map(lambda x: (x['CMPLNT_FR_DT'],  x['BORO_NM'] ,1))
    #np.save('daily_frequency.npy',np.array(days)

