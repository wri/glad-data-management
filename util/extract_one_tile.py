import zlib
import sqlite3

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import vector_tile_base

conn = sqlite3.connect('test.mbtiles')
cursor = conn.cursor()

sql = 'SELECT tile_data FROM tiles WHERE zoom_level = 14 AND tile_column = 6009 AND tile_row = 8005;'
cursor.execute(sql)

tile_data = [r[0] for r in cursor.fetchall()][0]
decompressed = zlib.decompress(str(tile_data), zlib.MAX_WBITS|16)

vt = vector_tile_base.VectorTile(decompressed).layers[0]

out_list = []

for feat in vt.features:
    out_list.append({'lat': feat.properties['save_lat'], 'lon': feat.properties['save_long'], 'julian_day': feat.properties['julian_day'], 'year': feat.properties['year']})


df = pd.DataFrame(out_list)

df['geometry'] = [Point(xy) for xy in zip(df.lon, df.lat)]
df = gpd.GeoDataFrame(df)

df.to_file('out.geojson', driver='GeoJSON')

