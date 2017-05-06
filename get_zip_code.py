import re
from pyspark.sql import Row
from pyspark.sql.functions import lit
from util import read_hdfs_csv, init_spark
import datetime
import numpy as np
from geopy.geocoders import Nominatim

#--packages com.databricks:spark-csv_2.11:1.5.0

def getZipCode(x):

    geolocator = Nominatim()

    coord = str(x['Latitude']) + ', ' + str(x['Longitude'])

    try:
        location = geolocator.reverse(coord)

        zip = location.address.split(',')[-2]
        zip = zip.strip()

        return x['CMPLNT_NUM'],zip

    except:

        return x['CMPLNT_NUM'],"99999"

if __name__ == '__main__':
        
    sc, sqlContext = init_spark(verbose_logging=True)

    rows = read_hdfs_csv(sqlContext, '/user/qg323/rows.csv')

    zip_codes = rows.select('CMPLNT_NUM' ,'Latitude' , 'Longitude')\
            .map(getZipCode)

    zip_codes = sqlContext.createDataFrame(zip_codes,['k',  'zip_code'])

    rows_zip = rows\
            .join(zip_codes, rows.CMPLNT_NUM == zip_codes.k, "left")\
            .drop("k")


    print(rows_zip.take(10))
