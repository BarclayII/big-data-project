
import re
from pyspark.sql import Row
from pyspark.sql.functions import lit, udf
from pyspark.sql.types import StringType
from util import read_hdfs_csv, init_spark, write_hdfs_csv
import datetime

def fullmatch(regex, s):
    return re.match(regex + '\\Z', s)

def isnull(x):
    return (
            x is None or                            # None
            (x == '') or                            # Empty string
            (isinstance(x, str) and x.isspace())    # Whitespace string
            )

def date_from_string(x):
    try:
        # The date info is pretty clean since it is always in MM/DD/YYYY form.
        # We leave the job of determining whether it is a valid date to
        # Python's datetime module:
        # (1) x.split() requires @x to be a string,
        # (2) int() requires each field to be an integer, separated by '/'
        # (3) datetime.date() requires the integers to form a valid date.
        month, day, year = x.split('/')
        date = datetime.date(int(year), int(month), int(day))
        return date
    except:         # TODO finer exceptions
        return None

def isdate(x):
    return date_from_string(x) is not None

def time_from_string(x):
    try:
        # Same logic as isdate()
        # A caveat is that 24:00:00 is also a valid time string so we treat it
        # separately
        hour, minute, second = [int(_) for _ in x.split(':')]
        if (hour == 24) and (minute == 0) and (second == 0):
            return datetime.time(0, 0, 0)
        time = datetime.time(hour, minute, second)
        return time
    except:         # TODO finer exceptions
        return None

def istime(x):
    return time_from_string(x) is not None

def datetime_from_string(date_str, time_str):
    try:
        month, day, year = [int(_) for _ in date_str.split('/')]
        hour, minute, second = [int(_) for _ in time_str.split(':')]
        date = datetime.date(year, month, day)
        if (hour == 24) and (minute == 0) and (second == 0):
            hour = 0
            date += datetime.timedelta(1)
        return datetime.datetime(date.year, date.month, date.day,
                                 hour, minute, second)
    except:
        return None

def assign_type(df):
    '''
    Insert a '_dtype' field for each column.  The value for each new column
    is the data type inferred from the corresponding column.  All values
    in a new column are the same.

    **IMPORTANT**: The order or columns may change.
    '''
    dtypes_dict = {
            'int': 'INT',
            'double': 'DECIMAL',
            'string': 'TEXT'
            }
    dtypes = dict(df.dtypes)
    cols = df.columns

    # Infer if the entire column is date/time, returns an array of bool
    isdatetime = (df
            .map(lambda r: [isdate(x) or istime(x) or isnull(x) for x in r])
            .reduce(lambda a, b: [x and y for x, y in zip(a, b)])
            )

    # TODO: I'm not sure if I should set KY_CD and PD_CD as INT or TEXT.
    # Technically it should be TEXT, but it can also be INT.
    for i, c in enumerate(cols):
        s = 'DATETIME' if isdatetime[i] else dtypes_dict[dtypes[c]]
        df = df.withColumn(c + '_dtype', lit(s))

    return df

def check_date_consistency(r):
    # TODO
    return False

# The semantics are assigned by manual inspection - because the columns are
# very clean, and does not have several types of inputs mixed together.
semantics = {
        "CMPLNT_NUM": "ID",
        "CMPLNT_FR_DT": "date",
        "CMPLNT_FR_TM": "time",
        "CMPLNT_TO_DT": "date",
        "CMPLNT_TO_TM": "time",
        "RPT_DT": "date",
        "KY_CD": "categorical (code)",
        "OFNS_DESC": "description",
        "PD_CD": "categorical (code)",
        "PD_DESC": "description",
        "CRM_ATPT_CPTD_CD": "categorical",
        "LAW_CAT_CD": "categorical",
        "JURIS_DESC": "categorical (department)",
        "BORO_NM": "categorical (borough location)",
        "ADDR_PCT_CD": "categorical (precinct number)",
        "LOC_OF_OCCUR_DESC":
            "categorical (inside/outside/rear of/opposite of/front of)",
        "PREM_TYP_DESC": "categorical (premise)",
        "PARKS_NM": "categorical (park name)",
        "HADEVELOPT": "categorical (housing development)",
        "X_COORD_CD": "New York Long Island SPCS X-coordinate",
        "Y_COORD_CD": "New York Long Island SPCS Y-coordinate",
        "Latitude": "Latitude",
        "Longitude": "Longitude",
        "Lat_Lon": "Latitude and Longitude",
        }

if __name__ == '__main__':
    sc, sqlContext = init_spark(verbose_logging='WARN')
    sc.addPyFile('util.py')

    rows = read_hdfs_csv(sqlContext, 'rows.csv')    # Change to your filename
    cols = rows.columns

    # (1) Assign data type for each column
    rows = assign_type(rows)

    # (2) Assign semantics for each column
    for c in cols:
        rows = rows.withColumn(c + '_sem', lit(semantics[c]))

    # Inconsistency checks:
    # (a) Make sure the IDs are unique:
    if rows.select('CMPLNT_NUM').distinct().count() != rows.count():
        # In practice we should print out which ID is not unique, but here we
        # have a very friendly dataset and this block never gets run.
        print 'The ID\'s are not unique'
    # Mark it valid
    rows = rows.withColumn('CMPLNT_NUM_valid', lit('VALID'))

    # (b) Make sure the TO_ date/time is after FR_ date/time
    inconsistent_date = rows.rdd.filter(check_date_consistency)
    inconsistent_date_count = inconsistent_date.count()
    if inconsistent_date_count > 0:
        print 'Number of inconsistent dates:', inconsistent_date_count

    # (c) Make sure the mapping between codes and descriptions are one-to-one
    # (Namely KY_CD and OFNS_DESC).
    ky_descs = (rows
                .select('KY_CD', 'OFNS_DESC')
                .distinct()
                .map(lambda r: (r['KY_CD'], [r['OFNS_DESC']]))
                .reduceByKey(lambda a, b: a + b)
                .collect())
    for k, descs in ky_descs:
        descs_not_null = [d for d in descs if not isnull(d)]
        if len(descs_not_null) < len(descs):
            num = (rows
                   .where((rows['KY_CD'] == k) & (rows['OFNS_DESC'] == ''))
                   .count())
            num_total = rows.where(rows['KY_CD'] == k).count()
            print ('Key code %03d has empty description in %d records out of %d' %
                    (k, num, num_total))
        if len(descs_not_null) > 1:
            print ('Key code %03d has multiple descriptions: %s' %
                   (k, descs_not_null))

    fill_null = udf(lambda s: 'NULL' if s == '' else 'VALID', StringType())
    rows = rows.withColumn('KY_CD_valid', lit('VALID'))
    rows = rows.withColumn('OFNS_DESC_valid', fill_null(rows.OFNS_DESC))

    # (d) Make sure the mapping between (KY_CD, PD_CD) and PD_DESC are
    # one-to-one
    pd_descs = (rows
                .select('KY_CD', 'PD_CD', 'PD_DESC')
                .distinct()
                .map(lambda r: ((r['KY_CD'], r['PD_CD']), [r['PD_DESC']]))
                .reduceByKey(lambda a, b: a + b)
                .collect())
    for k, descs in pd_descs:
        if len(descs) > 1:
            print '%s has multiple descriptions: %s' % (k, descs)
        elif k[1] is None:
            num = (rows
                   .where(rows['PD_CD'].isNull())
                   .count())
            print type(k[0]), type(k[1]), k
            print ('%d records found with key code %03d and no internal code' %
                   (num, k[0]))
        elif descs[0] == '':
            print '%s has empty description' % k

    # (e) Count the number of NULL values in different categorical variables
    for col in ['CRM_ATPT_CPTD_CD', 'LAW_CAT_CD', 'JURIS_DESC']:
        num = rows.where(rows[col].isNull()).count()
        if num > 0:
            print 'Column %s has %d empty values' % (col, num)
        distincts = rows.select(col).where(rows[col].isNotNull() & (rows[col] != '')).distinct().collect()
        print 'Column %s has distinct values %s' % (col, [d[col] for d in distincts])

    write_hdfs_csv(rows, 'rows-new.csv')
