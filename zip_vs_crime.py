
from polygon import *
import json
from util import *
import shapely.geometry as GEO

sc, sqlContext = init_spark(verbose_logging='INFO')
sc.addPyFile('polygon.py')

categories = {'FELONY': (0, 0, 1), 'MISDEMEANOR': (0, 1, 0), 'VIOLATION': (1, 0, 0)}

rows = read_hdfs_csv(sqlContext, 'rows.csv')
rows_latlon = rows.select('LAW_CAT_CD', 'Latitude', 'Longitude').dropna()
latlon = (
        rows_latlon
        .map(lambda r: (r.Longitude, r.Latitude, categories[r.LAW_CAT_CD]))
        .filter(lambda r: not (r[0] == '' or r[1] == ''))
        .map(lambda r: (float(r[0]), float(r[1]), r[2]))
        )

with open('zip.geojson') as f:
    zips = json.load(f)

zips_poly = fix_polygon(zips, lambda p: p['postalCode'])

zips_count = {}
for k in zips_poly:
    filtered = (
            latlon
            .filter(lambda p: zips_poly[k].contains(GEO.Point((p[0], p[1]))))
            )
    if filtered.count() > 0:
        zips_count[k] = (
                filtered
                .map(lambda r: r[2])
                .reduce(lambda a, b: tuple(x + y for x, y in zip(a, b)))
                )
    else:
        zips_count[k] = (0, 0, 0)

print zips_count
