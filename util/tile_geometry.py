import os

import fiona
import mercantile
from shapely.geometry import shape 


def est_area(tile_list, max_z):

    tile_area = 0.0

    for tile in tile_list:

        # tile area estimate
        # calculated with a value of 1 for the max_z tile 
        # so if max_z is 12, z12 tile is 1,
        # z11 tile is 4, z10 16, z9 64
        tile_area += 4 ** (max_z - tile.z) 

    return tile_area


def process_tile(tile_list, aoi, max_z):
    # main function to compare a list of tiles to an input geometry
    # will eventually return a list of tiles completely within the aoi (all zoom levels possible)
    # and tiles that intersect the aoi (must be of max_z because that's as low as we go)

    within_list = []
    intersect_list = []

    for t in tile_list:

        # a tile either is completely within, completely outside, or intersects
        tile_geom = shape(mercantile.feature(t)['geometry'])

        # if it's within, great- our work is done
        if aoi.contains(tile_geom):
            within_list.append(t)

        elif tile_geom.intersects(aoi):

            # if it intersects and is < max_z, subdivide it and start the 
            # process again
            if t.z < max_z:

                # find the four children of this tile and check them for within/intersect/outside-ness
                tile_children = mercantile.children(t)
                new_within_list, new_intersect_list = process_tile(tile_children, aoi, max_z)

                # add the results to our initial lists
                within_list.extend(new_within_list)
                intersect_list.extend(new_intersect_list)

            # if it intersects and is at max_z, add it to our intersect list
            else:
                intersect_list.append(t)

        # and if it's outside our geometry, drop it
        else:
            pass

    return within_list, intersect_list


def write_tiles_to_geojson(tile_list, output_file):

    schema={'geometry': 'Polygon', 'properties': {'title': 'str'}}

    # fiona can't overwrite existing files
    if os.path.exists(output_file):
        os.remove(output_file)

    with fiona.open(output_file, 'w', driver='GeoJSON', schema=schema) as outfile:
        for t in tile_list:
            feat = mercantile.feature(t)
            outfile.write(feat)


def build_tile_lists(geom, max_z, is_debug):

    # use bounds to find the smallest tile that completely contains our input aoi
    # not useful for AOIs that cross lat or lon 0 (returns tile [0, 0, 0])
    # but helpful for many AOIs
    # https://github.com/mapbox/mercantile/blob/master/docs/cli.rst#bounding-tile
    bbox = geom.bounds
    bounding_tile = mercantile.bounding_tile(*bbox)

    # build within and intersect lists
    within_list, intersect_list = process_tile([bounding_tile], geom, max_z)
    print '{} tiles in within_list, {} in intersect'.format(len(within_list), len(intersect_list))

    if is_debug:
        write_tiles_to_geojson(within_list, 'within.geojson')
        write_tiles_to_geojson(intersect_list, 'intersect.geojson')

    return within_list, intersect_list

