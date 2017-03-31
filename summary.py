
import re
from pyspark.sql import Row
from utils import read_hdfs_csv, init_spark

def fullmatch(regex, s):
    return re.match(regex + '\\Z', s)

def isnull(x):
    return (
            x is None or                            # None
            (x == '') or                            # Empty string
            (isinstance(x, str) and x.isspace())    # Whitespace string
            )

def isdate(x):
    try:
        # The date info is pretty clean since it is always in MM/DD/YYYY form.
        # We leave the job of determining whether it is a valid date to
        # Python's datetime module:
        # (1) x.split() requires @x to be a string,
        # (2) int() requires each field to be an integer, separated by '/'
        # (3) datetime.date() requires the integers to form a valid date.
        month, day, year = x.split('/')
        date = datetime.date(int(year), int(month), int(day))
        return True
    except:         # TODO finer exceptions
        return False

def istime(x):
    try:
        # Same logic as isdate()
        # A caveat is that 24:00:00 is also a valid time string so we treat it
        # separately
        hour, minute, second = [int(_) for _ in x.split(':')]
        if (hour == 24) and (minute == 0) and (second == 0):
            return True
        time = datetime.time(hour, minute, second)
        return True
    except:         # TODO finer exceptions
        return False

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

    def mapper(row,
               cols=cols,
               dtypes=dtypes,
               dtypes_dict=dtypes_dict,
               isdatetime=isdatetime):
        dic = row.asDict()
        for i, c in enumerate(cols):
            dic['%s_dtype' % c] = ('DATETIME' if isdatetime[i] 
                                  else dtypes_dict[dtypes[c]])
        return Row(**dic)

    return df.map(mapper)


if __name__ == '__main__':
    sc, sqlContext = init_spark(verbose_logging=True)

    rows = read_hdfs_csv(sqlContext, 'rows.csv')        # Change to your filename

    rows = assign_type(rows)
