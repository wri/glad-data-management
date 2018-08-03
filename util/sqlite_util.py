import sqlite3


def insert_intersect_table(cursor, tile_dict, tms=True):

    create_aoi_tiles_table(cursor)

    row_list = []

    # tile_list is a bunch of tile objects
    # from the mercantile library
    for tile, proportion_covered in tile_dict.iteritems():

        if tms:
            # need to convert each XYZ tile to TMS
            # vector tiles are indexed by TMS for some reason
            # https://gist.github.com/tmcw/4954720
            tile.y = (2 ** tile.z) - tile.y - 1

        row = [tile.x, tile.y, tile.z, proportion_covered]

        # append to row list for batch insert later
        row_list.append(row)

    cursor.executemany('INSERT INTO tiles_aoi values (?, ?, ?, ?)', row_list)


def create_aoi_tiles_table(cursor):

    # create or replace tiles_aoi table
    cursor.execute('DROP TABLE IF EXISTS tiles_aoi')

    create_table_sql = ('CREATE TABLE tiles_aoi ( '
                        'x INTEGER, '
                        'y INTEGER, '
                        'z INTEGER, '
                        'proportion_covered REAL, '
                        'PRIMARY KEY (x, y, z)); ')

    cursor.execute(create_table_sql)


def select_within_tiles(cursor):

    sql = ('SELECT alert_dict, proportion_covered '
           'FROM tile_summary_stats_z12 '
           'INNER JOIN tiles_aoi '
           'WHERE tile_summary_stats_z12.x = tiles_aoi.x AND tile_summary_stats_z12.y = tiles_aoi.y '
           'AND tile_summary_stats_z12.z = tiles_aoi.z ')

    cursor.execute(sql)
    rows = cursor.fetchall()

    return rows


def connect(sqlite_db):

    conn = sqlite3.connect(sqlite_db)
    cursor = conn.cursor()

    return conn, cursor

