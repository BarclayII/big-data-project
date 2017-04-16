
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
            x.isspace() or                          # Whitespace
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

def dump_info(df, col):
    with open(col, 'w') as f:
        df_sel = df.select(col, col + '_dtype', col + '_sem', col + '_valid').map(
                lambda r: ' '.join([
                    r[col],
                    r[col + '_dtype'],
                    r[col + '_sem'],
                    r[col + '_valid']
                    ])
                ).collect()
        for l in df_sel:
            f.write(l + '\n')

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

# We should treat the from date/time and to date/time as a whole because
# the validity of from date/time and to date/time depend on each other.
def handle_cmplnt_time(df, df_infer):
    df = assign_types_for_column(df, df_infer, 'CMPLNT_FR_DT')
    df = assign_types_for_column(df, df_infer, 'CMPLNT_FR_TM')
    df = assign_types_for_column(df, df_infer, 'CMPLNT_TO_DT')
    df = assign_types_for_column(df, df_infer, 'CMPLNT_TO_TM')

    def mapper(r):
        def formatter(key, frdt, frtm, todt, totm):
            return Row(
                    CMPLNT_NUM=key,
                    CMPLNT_FR_DT_valid=frdt,
                    CMPLNT_FR_TM_valid=frtm,
                    CMPLNT_TO_DT_valid=todt,
                    CMPLNT_TO_TM_valid=totm
                    )

        key = r['CMPLNT_NUM']
        df = str(r['CMPLNT_FR_DT'])
        tf = str(r['CMPLNT_FR_TM'])
        dt = str(r['CMPLNT_TO_DT'])
        tt = str(r['CMPLNT_TO_TM'])

        text_from = df + " " + tf
        text_to = dt + " " + tt

        mask = 0
        if not isnull(df):
            mask += 1000
        if not isnull(tf):
            mask += 100
        if not isnull(dt):
            mask += 10
        if not isnull(tt):
            mask += 1

        if mask == 1100 :
            result = "exact"
        elif mask == 1111 :
            result = "period"
        elif mask == 0011:
            result = "end-only"
        else:
            return formatter(key, 'INVALID', 'INVALID', 'INVALID', 'INVALID')

        time_from = None
        time_to = None

        if result != "end-only":
            time_from = datetime_from_string(df, tf)
            have_time_from = True
        else:
            have_time_from = False
        if result != "exact":
            time_to = datetime_from_string(dt, tt)
            have_time_to = True
        else:
            have_time_to = False
        
        if (time_from is None and have_time_from) or (time_to is None and have_time_to):
            return formatter(key, 'INVALID', 'INVALID', 'INVALID', 'INVALID')

        if result == "period":
            if time_to < time_from:
                return formatter(key, 'INVALID', 'INVALID', 'INVALID', 'INVALID')
            elif time_from == time_to:
                result = "exact"

        if result == "exact" :
            return formatter(key, 'VALID', 'VALID', 'NULL', 'NULL')
        if result == "period" :
            return formatter(key, 'VALID', 'VALID', 'VALID', 'VALID')
        if result == "end-only" :
            return formatter(key, 'NULL', 'NULL', 'VALID', 'VALID')

    datetime_valid = (df
            .select('CMPLNT_NUM', 'CMPLNT_FR_DT', 'CMPLNT_FR_TM', 'CMPLNT_TO_DT', 'CMPLNT_TO_TM')
            .map(mapper)
            .toDF()
            )

    return df.join(datetime_valid, on='CMPLNT_NUM')

def handle_rpt_dt(df, df_infer):
    df = assign_types_for_column(df, df_infer, 'RPT_DT')

    def _valid(s):
        if isnull(s):
            return 'NULL'
        elif date_from_string(s) == None :
            return 'INVALID'
        else:
            return 'VALID'

    valid = udf(_valid, StringType())
    return df.withColumn('RPT_DT_valid', valid(df.RPT_DT))

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
    df = assign_types_for_column(df, df_infer, 'CRM_ATPT_CPTD_CD')
    return handle_null_or_valid(df, 'CRM_ATPT_CPTD_CD')

def handle_law_cat_cd(df, df_infer):
    df = assign_types_for_column(df, df_infer, 'LAW_CAT_CD')
    return handle_null_or_valid(df, 'LAW_CAT_CD')

def handle_juris_desc(df, df_infer):
    df = assign_types_for_column(df, df_infer, 'JURIS_DESC')
    return handle_null_or_valid(df, 'JURIS_DESC')

# I have to handle the columns "Latitude", "Longitude" and "Lat_Lon" together,
# as handling each individually does not make sense: if a record has Longitude
# recorded but Latitude not recorded, then both Latitude and Longitude should
# be marked as "INVALID" rather than "NULL" for one and "VALID" for the other.
def handle_latlong(df, df_infer):
    def _valid(r):
        if isnull(r.Latitude) and isnull(r.Longitude):
            result = 'NULL'
        elif isnull(r.Latitude) or isnull(r.Longitude):
            result = 'INVALID'
        elif tryfloat(r.Latitude) is None or tryfloat(r.Longitude) is None:
            result = 'INVALID'
        else:
            result = 'VALID'

        if isnull(r.Lat_Lon):
            latlon_result = 'NULL'
        elif isnull(r.Latitude) or isnull(r.Longitude):
            latlon_result = 'INVALID'
        elif tryfloat(r.Latitude) is None or tryfloat(r.Longitude) is None:
            latlon_result = 'INVALID'
        elif r.Lat_Lon == '(%s, %s)' % (r.Latitude, r.Longitude):
            latlon_result = 'VALID'
        else:
            latlon_result = 'INVALID'
        return Row(
                CMPLNT_NUM=r.CMPLNT_NUM,
                Latitude_valid=result,
                Longitude_valid=result,
                Lat_Lon_valid=latlon_result
                )

    df = assign_types_for_column(df, df_infer, 'Latitude')
    df = assign_types_for_column(df, df_infer, 'Longitude')
    df = assign_types_for_column(df, df_infer, 'Lat_Lon')
    df_valid = df.map(_valid).toDF()
    return df.join(df_valid, on='CMPLNT_NUM')

def handle_addr_pct_cd(df, df_infer):
    df = assign_types_for_column(df, df_infer, 'ADDR_PCT_CD')

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
    df = assign_types_for_column(df, df_infer, 'BORO_NM')

    with open('Borough Boundaries.geojson') as f:
        boro_data = json.load(f)

    def _valid(s, set_):
        if isnull(s):
            return 'NULL'
        elif s not in set_:
            return 'INVALID'
        else:
            return 'VALID'

    boro_id = [f['properties']['boro_name'] for f in boro_data['features']]
    boro = fix_polygon(boro_data, lambda prop: prop['boro_name'].upper())

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

def handle_loc_of_occur_desc(df, df_infer):
    df = assign_types_for_column(df, df_infer, 'LOC_OF_OCCUR_DESC')
    return handle_null_or_valid(df, 'LOC_OF_OCCUR_DESC')

def handle_prem_typ_desc(df, df_infer):
    df = assign_types_for_column(df, df_infer, 'PREM_TYP_DESC')
    return handle_null_or_valid(df, 'PREM_TYP_DESC')

def handle_parks_nm(df, df_infer):
    df = assign_types_for_column(df, df_infer, 'PARKS_NM')
    return handle_null_or_valid(df, 'PARKS_NM')

def handle_hadevelopt(df, df_infer):
    df = assign_types_for_column(df, df_infer, 'HADEVELOPT')
    return handle_null_or_valid(df, 'HADEVELOPT')

# I have to process X_COORD_CD and Y_COORD_CD as a whole because processing
# each individually in this case does not make any sense.
def handle_coord_cd(df, df_infer):
    NAD83 = pyproj.Proj('''
        +proj=lcc +lat_1=41.03333333333333 +lat_2=40.66666666666666
        +lat_0=40.16666666666666 +lon_0=-74 +x_0=300000 +y_0=0
        +ellps=GRS80 +datum=NAD83 +units=m +no_defs
        ''')
    coord = df.map(
            lambda r:
            (
                r.CMPLNT_NUM,
                (
                    (float(r.Longitude), float(r.Latitude))
                    if not (isnull(r.Longitude) or isnull(r.Latitude))
                    else (None, None)
                ),(
                    NAD83(
                        ft2m(int(r.X_COORD_CD)),
                        ft2m(int(r.Y_COORD_CD)),
                        inverse=True
                        )
                    if not (isnull(r.X_COORD_CD) or isnull(r.Y_COORD_CD))
                    else (None, None)
                ),
                r.X_COORD_CD,
                r.Y_COORD_CD,
                r.Latitude,
                r.Longitude
            )
            )
    coord_diff = coord.map(
            lambda r: Row(
                COORD_DIST=((r[1][0] - r[2][0]) ** 2 + (r[1][1] - r[2][1]) ** 2) ** 0.5
                    if not (isnull(r[1][0]) or isnull(r[2][0]) or
                        isnull(r[1][1]) or isnull(r[2][1]))
                    else None,
                CMPLNT_NUM=r[0],
                X_COORD_CD=r[3],
                Y_COORD_CD=r[4],
                Latitude=r[5],
                Longitude=r[6]
                )
            )
    print coord_diff.filter(lambda x: x is None).count()
    print coord_diff.map(lambda r: r.COORD_DIST).filter(lambda x: x is not None).histogram(50)

    def _valid(r):
        if isnull(r.X_COORD_CD) and isnull(r.Y_COORD_CD):
            return 'NULL'
        elif isnull(r.X_COORD_CD) or isnull(r.Y_COORD_CD):
            return 'INVALID'
        elif r.COORD_DIST > 0.01:
            return 'CONFLICT'
        else:
            return 'VALID'

    df = assign_types_for_column(df, df_infer, 'X_COORD_CD')
    df = assign_types_for_column(df, df_infer, 'Y_COORD_CD')
    coord_valid = coord_diff.map(lambda r: Row(
            CMPLNT_NUM=r.CMPLNT_NUM,
            X_COORD_CD_valid=_valid(r),
            Y_COORD_CD_valid=_valid(r)
            )
            ).toDF()
    return df.join(coord_valid, on='CMPLNT_NUM')

dbname = 'rows.csv'

if __name__ == '__main__':
    sc, sqlContext = init_spark()
    sc.addPyFile('util.py')

    rows = read_hdfs_csv(sqlContext, dbname)
    rows_infer = read_hdfs_csv(sqlContext, dbname, inferschema=True)
    id_ = rows.select('CMPLNT_NUM')

    # CMPLNT_NUM
    df = handle_cmplnt_num(rows, rows_infer)
    summarize(df, 'CMPLNT_NUM')
    dump_info(df, 'CMPLNT_NUM')
    id_ = id_.intersect(df.where(df.CMPLNT_NUM_valid == 'VALID').select('CMPLNT_NUM'))

    # CMPLNT_FR_DT - CMPLNT_TO_TM
    df = handle_cmplnt_time(rows, rows_infer)
    for col in ['CMPLNT_FR_DT', 'CMPLNT_TO_DT', 'CMPLNT_FR_TM', 'CMPLNT_TO_TM']:
        summarize(df, col)
        dump_info(df, col)
        id_ = id_.intersect(df.where(df[col + '_valid'] != 'INVALID').select('CMPLNT_NUM'))

    # RPT_DT
    df = handle_rpt_dt(rows, rows_infer)
    summarize(df, 'RPT_DT')
    dump_info(df, 'RPT_DT')
    id_ = id_.intersect(df.where(df.RPT_DT_valid != 'INVALID').select('CMPLNT_NUM'))

    # KY_CD
    df = handle_ky_cd(rows, rows_infer)
    summarize(df, 'KY_CD')
    dump_info(df, 'KY_CD')

    # OFNS_DESC
    df = handle_ofns_desc(rows, rows_infer)
    summarize(df, 'OFNS_DESC')
    dump_info(df, 'OFNS_DESC')

    # PD_CD
    df = handle_pd_cd(rows, rows_infer)
    summarize(df, 'PD_CD')
    dump_info(df, 'PD_CD')

    # PD_DESC
    df = handle_pd_desc(rows, rows_infer)
    summarize(df, 'PD_DESC')
    dump_info(df, 'PD_DESC')

    # CRM_ATPT_CPTD_CD
    df = handle_crm_atpt_cptd_cd(rows, rows_infer)
    summarize_categories(df, 'CRM_ATPT_CPTD_CD')
    dump_info(df, 'CRM_ATPT_CPTD_CD')

    # LAW_CAT_CD
    df = handle_law_cat_cd(rows, rows_infer)
    summarize_categories(df, 'LAW_CAT_CD')
    dump_info(df, 'LAW_CAT_CD')

    # JURIS_DESC
    df = handle_juris_desc(rows, rows_infer)
    summarize_categories(df, 'JURIS_DESC')
    dump_info(df, 'JURIS_DESC')

    # Latitude, Longitude, Lat_Lon
    df = handle_latlong(rows, rows_infer)
    for col in ['Latitude', 'Longitude', 'Lat_Lon']:
        summarize(df, col)
        dump_info(df, col)

    df = handle_addr_pct_cd(rows, rows_infer)
    summarize(df, 'ADDR_PCT_CD')
    dump_info(df, 'ADDR_PCT_CD')

    df = handle_boro_nm(rows, rows_infer)
    summarize_categories(df, 'BORO_NM')
    dump_info(df, 'BORO_NM')

    df = handle_loc_of_occur_desc(rows, rows_infer)
    summarize(df, 'LOC_OF_OCCUR_DESC')
    dump_info(df, 'LOC_OF_OCCUR_DESC')

    df = handle_prem_type_desc(rows, rows_infer)
    summarize_categories(df, 'PREM_TYPE_DESC')
    dump_info(df, 'PREM_TYPE_DESC')

    df = handle_parks_nm(rows, rows_infer)
    summarize_categories(df, 'PARKS_NM')
    dump_info(df, 'PARKS_NM')

    df = handle_hadevelopt(rows, rows_infer)
    summarize_categories(df, 'HADEVELOPT')
    dump_info(df, 'HADEVELOPT')

    df = handle_coord_cd(rows, rows_infer)
    summarize(df, 'X_COORD_CD')
    summarize(df, 'Y_COORD_CD')
    dump_info(df, 'X_COORD_CD')
    dump_info(df, 'Y_COORD_CD')
