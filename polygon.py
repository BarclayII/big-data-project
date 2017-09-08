
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


def polydata(poly, color, stroke=None, fill=True, opacity=0.3, sw=1):
    data = GEO.mapping(poly)

    def json_code_point(lng, lat):
        return 'new google.maps.LatLng(%f, %f)' % (lat, lng)

    def json_code_pointset(points, name):
        return '''var %s = [%s];''' % (
                name,
                ',\n'.join([json_code_point(lng, lat) for lng, lat in points])
                )

    if data['type'] == 'Polygon':
        code = json_code_pointset(data['coordinates'][0], 'coords') + '''
var polygon = new google.maps.Polygon({
clickable: false,
geodesic: true,
fillColor: "%s",
fillOpacity: %f,
paths: coords,
strokeColor: "%s",
strokeOpacity: 1.000000,
strokeWeight: %s
});

polygon.setMap(map);''' % (color, opacity, color if stroke is None else stroke, sw)

    return code
