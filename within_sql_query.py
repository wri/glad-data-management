import argparse
import sqlite3

import fiona
from shapely.geometry import shape

from util import tile_geometry, sqlite_util, aoi_geom_intersect

parser = argparse.ArgumentParser(description='POC vector tile aoi point-in-polygon')
parser.add_argument('--polygon', '-p', help='the input AOI', required=True)
parser.add_argument('--max-z', '-z', type=int, help='the max z value of interest', required=True)
parser.add_argument('--debug', dest='debug', action='store_true')
args = parser.parse_args()


def main():

    # read in aoi
    src = fiona.open(args.polygon)
    geom = shape(src[0]['geometry'])

    max_z = args.max_z

    # find within_list, intersect_list
    # only concerned with intersect_list for this POC, but will ultimately
    # use within list as well
    within_list, intersect_list = tile_geometry.build_tile_lists(geom, max_z, args.debug)

    # connect to vector tiles / sqlite3 database
    conn, cursor = sqlite_util.connect('data_no_geom.mbtiles')

    intersect_area = tile_geometry.est_area(intersect_list, max_z)
    within_area = tile_geometry.est_area(within_list, max_z)

    area_ratio = intersect_area / (intersect_area + within_area)
    print 'area ratio is {}'.format(area_ratio)

    if area_ratio <= 0.05:

        print 'Estimated tile intersect area is <5% of within area!\n ' \
              'Starting to query the database based on date values.'

        # insert intersect list into mbtiles database as tiles_aoi
        # (this will be done in sqlite_util.py - remember to convert y --> tms_y)
        sqlite_util.insert_intersect_table(cursor, within_list, False)

        # query the database for summarized results
        #rows = sqlite_util.select_within_tiles(cursor)
        #print rows[0]
        print 'done'

    else:
        raise ValueError('geometry has >5% of area in intersecting tiles')



if __name__ == '__main__':
    main()

