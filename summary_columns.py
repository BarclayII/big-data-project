
import re
from pyspark.sql import Row
from pyspark.sql.functions import lit, udf
from pyspark.sql.types import StringType, BooleanType
from util import read_hdfs_csv, init_spark, write_hdfs_csv
import datetime
import json
import shapely.geometry as GEO
import pyproj

def tryfloat(s):
    try:
        return float(s)
    except:
        return None

def fullmatch(regex, s):
    return re.match(regex + '\\Z', s)

def ft2m(ft):
    return ft * 0.3048006096012192

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

def check_date_consistency(r):
    # TODO
    return False

def fix_polygon(poly_data, poly_key):
    for i, feat in enumerate(poly_data['features']):
        for j, coords in enumerate(feat['geometry']['coordinates']):
            if len(coords) == 1:
                coords.append([])
    poly = {poly_key(feat['properties']):
            GEO.MultiPolygon(feat['geometry']['coordinates'])
            for feat in poly_data['features']}
    return poly

def in_polygon(lat, long_, poly_id, poly_dict):
    lat = tryfloat(lat)
    long_ = tryfloat(long_)
    poly = poly_dict.get(poly_id, None)

    if (lat is None) and (long_ is None):
        point_null = True
    elif (lat is None) or (long_ is None):
        point_null = True
    else:
        point_null = False

    if isnull(poly_id):
        poly_valid = 'NULL'
        poly_null = True
    else:
        poly_null = False
        if point_null:
            if poly_id in poly_dict:
                poly_valid = 'VALID'
            else:
                poly_valid = 'INVALID'

    if poly_null or point_null:
        return poly_valid

    point = GEO.Point(long_, lat)
    poly = poly_dict.get(poly_id, None)
    if poly is None:
        poly_valid = 'INVALID'
    elif poly.contains(point):
        poly_valid = 'VALID'
    else:
        poly_valid = 'CONFLICT'
    return poly_valid

# The semantics are assigned by manual inspection - because the columns are
# very clean, and does not have several types of inputs mixed together.
semantics = {
        "CMPLNT_NUM": "ID",
        "CMPLNT_FR_DT": "date (starting from)",
        "CMPLNT_FR_TM": "time (starting from)",
        "CMPLNT_TO_DT": "date (ends at)",
        "CMPLNT_TO_TM": "time (ends at)",
        "RPT_DT": "date (of report)",
        "KY_CD": "categorical (key code)",
        "OFNS_DESC": "description",
        "PD_CD": "categorical (fine-grained key code)",
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

dtypes_dict = {
        'int': 'INT',
        'double': 'DECIMAL',
        'string': 'TEXT'
        }

dbname = 'rows.csv'     # Change to your file name

def assign_types_for_column(df, df_infer, col_name):
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
    dtypes = dict(df_infer.dtypes)
    col_id = [i for i in range(len(df.columns)) if df.columns[i] == col_name][0]

    # Infer if the entire column is date/time, returns an array of bool
    isdatetime = (df
            .map(lambda r: r[col_id])
            .map(lambda x: isdate(x) or istime(x) or isnull(x))
            .reduce(lambda a, b: a and b)
            )

    # TODO: I'm not sure if I should set KY_CD and PD_CD as INT or TEXT.
    # Technically it should be TEXT, but it can also be INT.
    s = 'DATETIME' if isdatetime else dtypes_dict[dtypes[col_name]]
    df = (df
            .withColumn(col_name + '_dtype', lit(s))
            .withColumn(col_name + '_sem', lit(semantics[col_name]))
            )

    return df

def handle_null_or_valid(df, col):
    def _null_or_valid(s):
        return 'NULL' if isnull(s) else 'VALID'
    null_or_valid = udf(_null_or_valid, StringType())
    return df.withColumn(col + '_valid', null_or_valid(df[col]))

def summarize_categories(df, col):
    print 'Summary for categorical column %s' % col
    print df.map(lambda r: r[col + '_valid']).countByValue()
    print df.map(lambda r: r[col]).filter(lambda x: not isnull(x)).countByValue()

def summarize(df, col):
    print 'Summary for column %s' % col
    print df.map(lambda r: r[col + '_valid']).countByValue()

# Invididual column handlers

def handle_cmplnt_num(df, df_infer):
    df = assign_types_for_column(df, df_infer, 'CMPLNT_NUM')

    df_valid = (df
            .map(lambda r: (r.CMPLNT_NUM, 1))
            .reduceByKey(lambda a, b: a + b)
            .map(lambda r: Row(
                CMPLNT_NUM=r[0],
                CMPLNT_NUM_valid='NULL' if isnull(r[0]) else ('VALID' if r[1] == 1 else 'INVALID')
                )
                )
            .toDF()
            )
    df = df.join(df_valid, on='CMPLNT_NUM')
    return df

def handle_cmplnt_fr_dt(df, df_infer):
    # TODO
    return df

def handle_cmplnt_fr_tm(df, df_infer):
    # TODO
    return df

def handle_cmplnt_to_dt(df, df_infer):
    # TODO
    return df

def handle_cmplnt_to_tm(df, df_infer):
    # TODO
    return df

def handle_rpt_dt(df, df_infer):
    # TODO
    return df

def handle_ky_cd(df, df_infer):
    df = assign_types_for_column(df, df_infer, 'KY_CD')

    def _valid(s):
        if isnull(s):
            return 'NULL'
        elif (len(s) != 3) or not s.isdigit():
            return 'INVALID'
        else:
            return 'VALID'

    valid = udf(_valid, StringType())
    return df.withColumn('KY_CD_valid', valid(df.KY_CD))

def handle_ofns_desc(df, df_infer):
    df = assign_types_for_column(df, df_infer, 'OFNS_DESC')

    ky_descs = dict(df
            .select('KY_CD', 'OFNS_DESC')
            .distinct()
            .map(lambda r: (
                r['KY_CD'],
                [r['OFNS_DESC']] if not isnull(r['OFNS_DESC']) else []
                )
                )
            .reduceByKey(lambda a, b: a + b)
            .collect()
            )
    for ky_cd, ofns_desc_list in ky_descs.items():
        if len(ofns_desc_list) > 1:
            print ky_cd, ofns_desc_list
    df_valid = (df
            .select('CMPLNT_NUM', 'KY_CD', 'OFNS_DESC')
            .map(lambda r: Row(
                CMPLNT_NUM=r['CMPLNT_NUM'],
                OFNS_DESC_valid=(
                    'NULL' if isnull(r['OFNS_DESC']) else
                    ('CONFLICT' if len(ky_descs[r['KY_CD']]) > 1 else 'VALID')
                    )
                ))
            .toDF()
            )
    return df.join(df_valid, on='CMPLNT_NUM')

def handle_pd_cd(df, df_infer):
    df = assign_types_for_column(df, df_infer, 'PD_CD')

    def _valid(s):
        if isnull(s):
            return 'NULL'
        elif (len(s) != 3) or not s.isdigit():
            return 'INVALID'
        else:
            return 'VALID'

    valid = udf(_valid, StringType())
    return df.withColumn('PD_CD_valid', valid(df.PD_CD))

def handle_pd_desc(df, df_infer):
    df = assign_types_for_column(df, df_infer, 'PD_DESC')

    pd_descs = dict(df
            .select('KY_CD', 'PD_CD', 'PD_DESC')
            .distinct()
            .map(lambda r: (
                (r['KY_CD'], r['PD_CD']),
                [r['PD_DESC']] if not isnull(r['PD_DESC']) else []
                )
                )
            .reduceByKey(lambda a, b: a + b)
            .collect()
            )
    for (ky_cd, pd_cd), pd_desc_list in pd_descs.items():
        if len(pd_desc_list) > 1:
            print ky_cd, pd_cd, pd_desc_list
    df_valid = (df
            .select('CMPLNT_NUM', 'KY_CD', 'PD_CD', 'PD_DESC')
            .map(lambda r: Row(
                CMPLNT_NUM=r['CMPLNT_NUM'],
                PD_DESC_valid=(
                    'NULL' if isnull(r['PD_DESC']) else
                    ('CONFLICT' if len(pd_descs[(r['KY_CD'], r['PD_CD'])]) > 1 else 'VALID')
                    )
                ))
            .toDF()
            )
    return df.join(df_valid, on='CMPLNT_NUM')

def handle_crm_atpt_cptd_cd(df, df_infer):
    return handle_null_or_valid(df, 'CRM_ATPT_CPTD_CD')

def handle_law_cat_cd(df, df_infer):
    return handle_null_or_valid(df, 'LAW_CAT_CD')

def handle_juris_desc(df, df_infer):
    return handle_null_or_valid(df, 'JURIS_DESC')

def handle_latitude(df, df_infer):
    return handle_null_or_valid(df, 'Latitude')

def handle_longitude(df, df_infer):
    return handle_null_or_valid(df, 'Longitude')

def handle_addr_pct_cd(df, df_infer):
    with open('Police Precincts.geojson') as f:
        prec_data = json.load(f)

    def _valid(s, set_):
        if isnull(s):
            return 'NULL'
        elif s not in set_:
            return 'INVALID'
        else:
            return 'VALID'

    prec_id = [f['properties']['precinct'] for f in prec_data['features']]
    prec = fix_polygon(prec_data, lambda prop: prop['precinct'])

    df_in_prec = df.map(
            lambda r: Row(
                CMPLNT_NUM=r.CMPLNT_NUM,
                ADDR_PCT_CD_valid=in_polygon(
                    r.Latitude, r.Longitude, r.ADDR_PCT_CD, prec
                    )
                )
            ).toDF()
    _old_count = df.count()
    df = df.join(df_in_prec, on='CMPLNT_NUM')
    assert df.count() == _old_count

    return df

def handle_boro_nm(df, df_infer):
    with open('Borough Boundaries.geojson') as f:
        boro_data = json.load(f)

    def _valid(s, set_):
        if isnull(s):
            return 'NULL'
        elif s not in set_:
            return 'INVALID'
        else:
            return 'VALID'

    boro_id = [f['properties']['precinct'] for f in boro_data['features']]
    boro = fix_polygon(boro_data, lambda prop: prop['precinct'])

    df_in_boro = df.map(
            lambda r: Row(
                CMPLNT_NUM=r.CMPLNT_NUM,
                BORO_NM_valid=in_polygon(
                    r.Latitude, r.Longitude, r.BORO_NM, boro
                    )
                )
            ).toDF()
    _old_count = df.count()
    df = df.join(df_in_boro, on='CMPLNT_NUM')
    assert df.count() == _old_count

    return df

dbname = 'rows.csv'

if __name__ == '__main__':
    sc, sqlContext = init_spark()
    sc.addPyFile('util.py')

    rows = read_hdfs_csv(sqlContext, dbname)
    rows_infer = read_hdfs_csv(sqlContext, dbname, inferschema=True)

    summarize(handle_addr_pct_cd(rows, rows_infer), 'ADDR_PCT_CD')
