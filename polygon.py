
import shapely.geometry as GEO
from numbers import Number

def fix_polygon(poly_data, poly_key):
    poly = {}
    for i, feat in enumerate(poly_data['features']):
        if feat['geometry']['type'] == 'Polygon':
            coords = feat['geometry']['coordinates']
            if len(coords) == 1:
                coords.append([])
            else:
                if not hasattr(coords[1][0][0], '__len__'):
                    coords[1] = [coords[1]]
            poly[poly_key(feat['properties'])] = GEO.Polygon(*coords)
        elif feat['geometry']['type'] == 'MultiPolygon':
            for j, coords in enumerate(feat['geometry']['coordinates']):
                if len(coords) == 1:
                    coords.append([])
                else:
                    if not hasattr(coords[1][0][0], '__len__'):
                        coords[1] = [coords[1]]
            poly[poly_key(feat['properties'])] = GEO.MultiPolygon(feat['geometry']['coordinates'])
    return poly
