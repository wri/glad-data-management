import os
from functools import partial

from shapely.geometry import shape
from shapely.ops import transform
import fiona
import mercantile
import pyproj



def calc_area(geom, proj=None, init=None):

    proj_kwargs = {'lat1': geom.bounds[1],
                   'lat2': geom.bounds[3]}

    if proj:
        proj_kwargs['proj'] = proj
    elif init:
        proj_kwargs['init'] = init
    else:
        raise ValueError('must specify either proj or init string')

    # source: https://gis.stackexchange.com/a/166421/30899
    geom_projected = transform(
        partial(
            pyproj.transform,
            pyproj.Proj(init='EPSG:4326'),
            pyproj.Proj(**proj_kwargs)),
        geom)

    # return area in ha
    return geom_projected.area / 10000.


def get_intersect_area(aoi, t):

    tile_geom = shape(mercantile.feature(t)['geometry'])
    intersect_geom = aoi.intersection(tile_geom)

    return calc_area(intersect_geom, init="EPSG:3857")


def process_tile(tile_list, aoi):
    # main function to compare a list of tiles to an input geometry
    # will eventually return a list of tiles completely within the aoi (all zoom levels possible)
    # and tiles that intersect the aoi (must be of max_z because that's as low as we go)

    within_list = []
    intersect_list = []

    max_z = 12

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
                new_within_list, new_intersect_list = process_tile(tile_children, aoi)

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


def write_tiles_to_geojson(tile_dict, output_file):

    schema={'geometry': 'Polygon', 'properties': {'title': 'str', 'proportion_covered': 'float'}}

    # fiona can't overwrite existing files
    if os.path.exists(output_file):
        os.remove(output_file)

    with fiona.open(output_file, 'w', driver='GeoJSON', schema=schema) as outfile:
        for t, proportion_covered in tile_dict.iteritems():
            feat = mercantile.feature(t)
            feat['properties']['proportion_covered'] = proportion_covered
            outfile.write(feat)


def build_tile_dict(geom, is_debug):

    # use bounds to find the smallest tile that completely contains our input aoi
    # not useful for AOIs that cross lat or lon 0 (returns tile [0, 0, 0])
    # but helpful for many AOIs
    # https://github.com/mapbox/mercantile/blob/master/docs/cli.rst#bounding-tile
    bbox = geom.bounds
    bounding_tile = mercantile.bounding_tile(*bbox)

    # divide tiles into within and intersecting lists
    within_list, intersect_list = process_tile([bounding_tile], geom)

    # initialize tile: proportion covered dict, starting with within
    # tiles, all of which have a coverage proportion of 1 
    tile_dict = dict([(x, 1) for x in within_list])

    for t in intersect_list:

        # do intersection of intersecting tile and original AOI geom
        intersect_area = get_intersect_area(geom, t)

        # divide intersect area by area of all z12 tiles in webmerc
        tile_dict[t] = intersect_area / 9572.547449763457

    if is_debug:
        write_tiles_to_geojson(tile_dict, 'debug.geojson')

    return tile_dict

