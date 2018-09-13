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

    # create temp table
    create_table_sql = ('CREATE TEMPORARY TABLE tiles_aoi ( '
                        'x INTEGER, '
                        'y INTEGER, '
                        'z INTEGER, '
                        'proportion_covered REAL) ')
    cursor.execute(create_table_sql)

    # add index on x - trying to increase cardinality for easy searching
    # previously had (x,y,z) as primary key - while true, it was very slow
    cursor.execute('CREATE INDEX tiles_aoi_idx_x ON tiles_aoi(x)')


def select_within_tiles(cursor):

    sql = ('SELECT 1, CAST(SUM(proportion_covered * alert_count) as integer)'
           'FROM unpacked '
           'INNER JOIN tiles_aoi '
           'WHERE unpacked.x = tiles_aoi.x AND unpacked.y = tiles_aoi.y AND unpacked.z = tiles_aoi.z ')

    cursor.execute(sql)
    rows = cursor.fetchall()

    return rows


def connect():

    conn = sqlite3.connect('all.db')
    cursor = conn.cursor()

    return conn, cursor

