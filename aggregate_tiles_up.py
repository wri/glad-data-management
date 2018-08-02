import argparse
import datetime
import sqlite3
import zlib
import json
from collections import Counter
import csv
import multiprocessing as mp

import numpy as np
import pandas as pd
import mercantile
import vector_tile_base

from util import multiprocessing_mapreduce

parser = argparse.ArgumentParser(description='POC script to aggregate high zooms to low')
parser.add_argument('mbtiles_db', help='the mbtiles database')
args = parser.parse_args()


def main():

    # query the sqlite database
    print args
    conn = sqlite3.connect(args.mbtiles_db)
    cursor = conn.cursor()

    print 'starting sql read'
    sql = 'SELECT zoom_level as z, tile_row as y, tile_column as x FROM tiles' 
    cursor.execute(sql)
    rows = np.array(cursor.fetchall())

    # divide into equal chunks based on number of processors
    cpu_count = mp.cpu_count() - 2
    l = np.array_split(rows, cpu_count)

    mapper = multiprocessing_mapreduce.SimpleMapReduce(map_tile_to_parent, combine_alert_stats, cpu_count)
    results = mapper(l)

    print 'writing results back to sqlite database'

    # to view a small subset of these tiles on geojsonio, export table to CSV, then run:
    # awk  -F',' 'BEGIN{OFS=",";} {print "[" $1,$2,$3 "]"; }' output.csv | grep -v 'x,y,z' | mercantile shapes | fio collect | geojsonio
    row_list = []

    for row in results:
        tile_tuple, date_dict = row
        z, y, x = tile_tuple

        out_row = [z, y, x, json.dumps(date_dict)]
        row_list.append(out_row)

    df = pd.DataFrame(row_list, columns=['x', 'y', 'z', 'alert_dict'])
    df.set_index(['x', 'y', 'z'], inplace=True)

    df.to_sql('tile_summary_stats_z12', conn, if_exists='replace')


def map_tile_to_parent(input_tile_list):

    conn = sqlite3.connect(args.mbtiles_db)
    cursor = conn.cursor()

    output_list = []

    for input_tile in input_tile_list:

        print mp.current_process().name, 'querying db for', input_tile
        z, y, x = input_tile

        sql = 'SELECT tile_data FROM tiles WHERE zoom_level = {} AND tile_row = {} and tile_column = {}'.format(z, y, x)
        cursor.execute(sql)

        tile_data = [r[0] for r in cursor.fetchall()][0]

        # and decode the associated GLAD point data as well
        decompressed = zlib.decompress(str(tile_data), zlib.MAX_WBITS|16)
        vt = vector_tile_base.VectorTile(decompressed).layers[0]

        date_dict = {}

        for feat in vt.features:
            as_date = convert_jd(feat.properties['year'], feat.properties['julian_day'])
            conf = feat.properties['confidence']

            # aggregating by (date, conf) tuples
            # can't serialize a tuples as a key, must be string instead
            key = '{}::{}'.format(as_date, conf)

            # if we already have a GLAD alert for this date, add to our count
            # otherwise create a new entry in the dict
            try:
                date_dict[key] += 1
            except KeyError:
                date_dict[key] = 1

        # convert TMS y to XYZ y
        # http://bl.ocks.org/lennepkade/b6fe9e4862668b2d19fe26f6c2d7cbef
        xyz_y = ((1 << z) - y - 1)

        # save the highest zoom level tile to our output list
        output_list.append(((x, xyz_y, z), date_dict))

        # aggregate this tile all the way up, saving all parents to our output list as well
        output_list.extend(add_parent_tiles(x, xyz_y, z, date_dict))

    return output_list
    

def add_parent_tiles(child_x, child_y, child_z, date_dict):

    output_list = []

    for i in range(child_z, 4, -1):
        parent = mercantile.parent(child_x, child_y, child_z)

        output_list.append(((parent.x, parent.y, parent.z), date_dict))

        child_x, child_y, child_z = parent.x, parent.y, parent.z

    return output_list


def combine_alert_stats(item):

    tile_id, dict_list = item
    print mp.current_process().name, 'combining tile_id',  tile_id

    # https://stackoverflow.com/a/11290471/4355916
    # 4 tiles contribute to each parent tile, so need to group them by tile_id
    # and add date dictionaries
    return tile_id, sum((Counter(dict(x)) for x in dict_list), Counter())


def convert_jd(year, julian_day):
    as_dt = datetime.datetime(year, 1, 1) + datetime.timedelta(julian_day - 1)

    return as_dt.strftime('%Y-%m-%d')


if __name__ == '__main__':
    main()


