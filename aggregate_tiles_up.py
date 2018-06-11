import argparse
import datetime
import sqlite3
import zlib
from collections import Counter
import csv
import multiprocessing as mp

import mercantile
import vector_tile_base

from util import multiprocessing_mapreduce

parser = argparse.ArgumentParser(description='POC script to aggregate high zooms to low')
parser.add_argument('--mbtiles-db', '-m', help='the mbtiles database', required=True)
args = parser.parse_args()


def main():

    # query the sqlite database
    conn = sqlite3.connect(args.mbtiles_db)
    cursor = conn.cursor()

    sql = 'SELECT zoom_level as z, tile_row as y, tile_column as x FROM tiles' 
    cursor.execute(sql)
    l = [cursor.fetchall()]

    cpu_count = mp.cpu_count() - 2
    mapper = multiprocessing_mapreduce.SimpleMapReduce(map_tile_to_parent, combine_alert_stats, 1)
    results = mapper(l)

    with open('output.csv', 'w') as dst:
        csv_writer = csv.writer(dst)
        csv_writer.writerow(['z', 'y', 'x', 'alert_date', 'alert_count'])

        for row in results:
            tile_tuple, date_dict = row
            z, y, x = tile_tuple

            for date_str, alert_count in date_dict.iteritems():
                out_row = [z, y, x, date_str, alert_count]
                csv_writer.writerow(out_row)


def map_tile_to_parent(input_tile_list):

    conn = sqlite3.connect(args.mbtiles_db)
    cursor = conn.cursor()

    output_list = []

    for input_tile in input_tile_list:

        z, y, x = input_tile

        sql = 'SELECT tile_data FROM tiles WHERE zoom_level = {} AND tile_row = {} and tile_column = {}'.format(z, y, x)
        cursor.execute(sql)

        tile_data = [r[0] for r in cursor.fetchall()][0]

        # and decode the associated GLAD point data as well
        decompressed = zlib.decompress(str(tile_data), zlib.MAX_WBITS|16)
        vt = vector_tile_base.VectorTile(decompressed).layers[0]

        date_dict = {}

        for feat in vt.features:
            #date_tuple = (feat.properties['year'], feat.properties['julian_day'])
            as_date = convert_jd(feat.properties['year'], feat.properties['julian_day'])

            # if we already have a GLAD alert for this date, add to our count
            # otherwise create a new entry in the dict
            try:
                date_dict[as_date] += 1
            except KeyError:
                date_dict[as_date] = 1

        # convert TMS y to XYZ y
        # http://bl.ocks.org/lennepkade/b6fe9e4862668b2d19fe26f6c2d7cbef
        xyz_y = ((1 << z) - y - 1)

        # find the parent for our map function
        parent = mercantile.parent(x, xyz_y, z)

        output_list.append(((parent.x, parent.y, parent.z), date_dict))

    return output_list
    

def combine_alert_stats(item):
    tile_id, dict_list = item


    # https://stackoverflow.com/a/11290471/4355916
    return tile_id, sum((Counter(dict(x)) for x in dict_list), Counter())


def convert_jd(year, julian_day):
    as_dt = datetime.datetime(year, 1, 1) + datetime.timedelta(julian_day - 1)

    return as_dt.strftime('%Y-%m-%d')


if __name__ == '__main__':
    main()


