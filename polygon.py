
import shapely.geometry as GEO

def fix_polygon(poly_data, poly_key):
    poly = {}
    for i, feat in enumerate(poly_data['features']):
        if feat['geometry']['type'] == 'Polygon':
            coords = feat['geometry']['coordinates']
            poly[poly_key(feat['properties'])] = GEO.Polygon(*feat['geometry']['coordinates'])
        elif feat['geometry']['type'] == 'MultiPolygon':
            for j, coords in enumerate(feat['geometry']['coordinates']):
                if len(coords) == 1:
                    coords.append([])
            poly[poly_key(feat['properties'])] = GEO.MultiPolygon(feat['geometry']['coordinates'])
    return poly
