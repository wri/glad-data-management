import sqlite3


def insert_intersect_table(cursor, tile_list):

    create_aoi_tiles_table(cursor)

    row_list = []

    # tile_list is a bunch of tile objects
    # from the mercantile library
    for tile in tile_list:

        # need to convert each XYZ tile to TMS
        # vector tiles are indexed by TMS for some reason
        # https://gist.github.com/tmcw/4954720
        # hardcode zoom level 12 - only zoom we're working with
        tms_y = (2 ** 12) - tile.y - 1
        row = [tile.x, tms_y]

        # append to row list for batch insert later
        row_list.append(row)

    cursor.executemany('INSERT INTO tiles_aoi values (?, ?)', row_list)


def create_aoi_tiles_table(cursor):

    # create or replace tiles_aoi table
    cursor.execute('DROP TABLE IF EXISTS tiles_aoi')

    create_table_sql = ('CREATE TABLE tiles_aoi ( '
                        'x integer, '
                        'y integer, '
                        'PRIMARY KEY (x, y)); ')

    cursor.execute(create_table_sql)


def select_intersected_tiles(cursor):

    # query the vector tiles table (tiles)
    # selecting those tiles that match x & y indexes with tiles_aoi

    sql = ('SELECT tiles_aoi.x, tiles_aoi.y, tile_data '
           'FROM tiles '
           'INNER JOIN tiles_aoi '
           'WHERE tiles.tile_row = tiles_aoi.y and tiles.tile_column = tiles_aoi.x;')

    cursor.execute(sql)
    rows = cursor.fetchall()

    return rows


def connect(sqlite_db):

    conn = sqlite3.connect(sqlite_db)
    cursor = conn.cursor()

    return conn, cursor
