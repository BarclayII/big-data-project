import re
from pyspark.sql import Row
from pyspark.sql.functions import lit
from util import read_hdfs_csv, init_spark
import datetime
import numpy as np

#--packages com.databricks:spark-csv_2.11:1.5.0


'''
root
 |-- DBN: string (nullable = true)
  |-- SCHOOL NAME: string (nullable = true)
   |-- Num of SAT Test Takers: string (nullable = true)
    |-- SAT Critical Reading Avg. Score: string (nullable = true)
     |-- SAT Math Avg. Score: string (nullable = true)
      |-- SAT Writing Avg. Score: string (nullable = true)
'''

def total_score(x):

    try:
        x = [float(_x) for _x in x]
    except:
        return( (0,0,0,0,0)  )

    n_students = x[0]
    
    return((n_students , x[1] * n_students, x[2] * n_students, x[3] * n_students , 1 ))

def avg(x):

    n_students = x[0]

    if n_students ==0:
        return(0,0,0)

    return( x[1] / n_students , x[2] / n_students , x[3] / n_students  )

if __name__ == '__main__':
        
    sc, sqlContext = init_spark(verbose_logging=True)

    sat = read_hdfs_csv(sqlContext, '/user/zz1409/sat_scores2012.csv')
    loc = read_hdfs_csv(sqlContext, '/user/zz1409/loc_clean.csv')

    sat= sat.map(lambda x: (x[1].lower(), (x[2],x[3],x[4] ,x[5])) )

    loc = loc.select( 'school_name' , 'zip').map(lambda x: (x[0],x[1]))

    df = sat.join(loc).map( lambda x: (x[1][1] , total_score( x[1][0]  )   ))
    df = df.reduceByKey( lambda x,y: [ x+y for x,y in zip(x,y) ] )
    df = df.map(lambda x: ( x[0] , avg(x[1]) ))
    df = df.map(lambda x: (x[0], x[1][0],x[1][1],x[1][2]  ))

    df = df.toDF(['zip', 'reading','math','writing'])
    df.toPandas().to_csv('mycsv.csv')


    sc.stop()

