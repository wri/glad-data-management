import numpy as np
import rasterio
from shapely.geometry import shape

from geop import geo_utils


def download_points(geojson):

    geom = shape(geojson[0]['geometry'])

    raster_src = '/vsis3/palm-risk-poc/data/glad/data.vrt'

    with rasterio.open(raster_src) as src:
        window, shifted_affine = geo_utils.get_window_and_affine(geom, src)
        data = src.read(1, masked=True, window=window)

        i, j = np.where(data)
        masked_x = j * .00025 + shifted_affine.xoff + 0.000125
        masked_y = i * -.00025 + shifted_affine.yoff - 0.000125

        yield 'longitude,latitude,gladval'
        for x, y, z in zip(masked_x, masked_y, data.compressed()):
            yield '{},{},{}'.format(x, y, z)


if __name__ == '__main__':

    geojson = {"type":"FeatureCollection","features":[{"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[-51,0.75],[-50.75,0.75],[-50.75,1],[-51,1],[-51,0.75]]]}}]} 

    for row in download_points(geojson['features']):
        print row

