import argparse
import json
import sqlite3

import fiona
import requests
from shapely.geometry import shape

from util import tile_geometry, sqlite_util, aoi_geom_intersect, util


def calc_stats(geojson, max_z=12, debug=False):    

    geom = shape(geojson[0]['geometry'])

    # find within_list, intersect_list
    # only concerned with intersect_list for this POC, but will ultimately
    # use within list as well
    within_list, intersect_list = tile_geometry.build_tile_lists(geom, max_z, debug)

    
    # connect to vector tiles / sqlite3 database
    conn, cursor = sqlite_util.connect('data_no_geom_z11_12.mbtiles')

    intersect_area = tile_geometry.est_area(intersect_list, max_z, debug)
    within_area = tile_geometry.est_area(within_list, max_z, debug)

    area_ratio = intersect_area / (intersect_area + within_area)
    print 'area ratio is {}'.format(area_ratio)

    if area_ratio <= 0.05:

        print 'Estimated tile intersect area is <5% of within area!\n ' \

        # insert intersect list into mbtiles database as tiles_aoi
        # (this will be done in sqlite_util.py - remember to convert y --> tms_y)
        sqlite_util.insert_intersect_table(cursor, within_list, False)

        # query the database for summarized results
        rows = sqlite_util.select_within_tiles(cursor)

        # combine rows into one dictionary
        alert_date_dict = util.row_list_to_json(rows)

        if alert_date_dict:
            return alert_date_dict.items()[0]
        else:
            return {}

    else:
        print 'geometry has >5% of area in intersecting tiles, trying lambda endpoint'
        url = 'https://3bkj4476d9.execute-api.us-east-1.amazonaws.com/dev/glad-alerts'
        headers = {"Content-Type": "application/json"}
        payload = json.dumps({'geojson': {'type': 'FeatureCollection', 'features': [geojson[0]]}})

        r = requests.post(url, data=payload, headers=headers)

        return r.json()
        

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='POC vector tile aoi point-in-polygon')
    parser.add_argument('--polygon', '-p', help='the input AOI', required=True)
    parser.add_argument('--max-z', '-z', type=int, help='the max z value of interest', required=True)
    parser.add_argument('--debug', dest='debug', action='store_true')
    args = parser.parse_args()

    # read in aoi
    src = fiona.open(args.polygon)

    resp = calc_stats(src, args.max_z, args.debug)

    print resp


