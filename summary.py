
import re
from pyspark.sql import Row
from pyspark.sql.functions import lit
from util import read_hdfs_csv, init_spark
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

if __name__ == '__main__':
    sc, sqlContext = init_spark(verbose_logging=True)
    sc.addPyFile('util.py')

    rows = read_hdfs_csv(sqlContext, 'rows.csv')    # Change to your filename

    # (1) Assign data type for each row
    rows = assign_type(rows)

    # Inconsistency checks:
    # (a) Make sure the IDs are unique:
    if rows.select('CMPLNT_NUM').distinct().count() != rows.count():
        # In practice we should print out which ID is not unique, but here we
        # have a very friendly dataset and this block never gets run.
        print 'The ID\'s are not unique'

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
                   .select('CMPLNT_NUM', 'KY_CD', 'OFNS_DESC')
                   .where((rows['KY_CD'] == k) & (rows['OFNS_DESC'] == ''))
                   .count())
            print 'Key code %03d has empty description in %d records' % (k, num)
        if len(descs_not_null) > 1:
            print ('Key code %03d has multiple descriptions: %s' %
                   (k, descs_not_null))

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
                   .select('CMPLNT_NUM', 'KY_CD', 'PD_CD')
                   .where(rows['PD_CD'].isNull())
                   .count())
            print ('%d records found with key code %03d and no internal code' %
                   (num, k[0]))
        elif descs[0] == '':
            print '%s has empty description' % k
