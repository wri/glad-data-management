import json
import zlib
import sqlite3

from shapely.geometry import shape, mapping, Point
#import mapbox_vector_tile
import vector_tile_base


def main():

    # will need to be dynamic based on intersect.geojson at some point
    rows = get_vt_data()
    xy_set = set([(row[0], row[1]) for row in rows])

    with open('data/aoi.geojson') as src:
        aoi = json.load(src)
        aoi_geom = shape(aoi['features'][0]['geometry'])

    # builds dictionary of (tile_x, tile_y) : geojson
    # for all the z12 tiles that split the AOI
    tile_dict = build_intersect_tile_dict() 

    point_list = []
    poly_list = []

    for row in rows:
        xy_tuple = (row[0], row[1])

        full_tile_geom = shape(tile_dict[xy_tuple]['geometry'])
        tile_aoi_geom = full_tile_geom.intersection(aoi_geom)

        poly_list.append(tile_aoi_geom)

        # decompress the tile data
        decompressed = zlib.decompress(str(row[2]), zlib.MAX_WBITS|16)
        #vt = mapbox_vector_tile.decode(decompressed)['filt_sa_onlycsv']
        vt = vector_tile_base.VectorTile(decompressed).layers[0]

        point_count = 0
        within_count = 0

        for feat in vt.features:
            point_count += 1
            p = Point(feat.properties['lon_prop'], feat.properties['lat_prop'])

            if tile_aoi_geom.contains(p):
                within_count += 1
            #print json.dumps(mapping(p))
            #print json.dumps(mapping(tile_aoi_geom))

        print 'for tile xy {}, {} of {} points were inside'.format(xy_tuple, within_count, point_count)



def build_intersect_tile_dict():
    
    tile_dict = {}

    with open('data/intersect_cols_unpacked.geojson') as src:
        intersect_fc = json.load(src)

        for tile in intersect_fc['features']:

            # check if tile exists in sqlite database
            tile_id = (tile['properties']['x'], tile['properties']['tms_y']) 

            tile_dict[tile_id] = tile

    return tile_dict


def get_vt_data():

    # query the vector tiles table (tiles)
    # selecting those tiles that match x & y indexes with the pre-created
    # tiles_aoi table
    # hardcoded for testing, will ultimately do this dynamically
    conn = sqlite3.connect('data/sa_all.mbtiles')
    cursor = conn.cursor()

    sql = ('SELECT tiles_aoi.x, tiles_aoi.y, tile_data '
           'FROM tiles '
           'INNER JOIN tiles_aoi '
           'WHERE tiles.tile_row = tiles_aoi.y and tiles.tile_column = tiles_aoi.x;')

    cursor.execute(sql)
    rows = cursor.fetchall()

    conn.close()

    return rows


if __name__ == '__main__':
    main()
