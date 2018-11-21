### Yearly Update

##### General update info
Most of the difficult work is done by the [mapnik-forest-change-tiles](https://github.com/wri/mapnik-forest-change-tiles) process- adding the recently updated date and confidence raster data (currently for year 2018) to the old date + confidence raster (data for years 2015 - 2017).

For each individual 10x10 tile, the [prep_previous_years function](https://github.com/wri/mapnik-forest-change-tiles/blob/master/raster_processing/pre_processing.py#L155) checks the `pre_processed` folder to see if the preprocessed data from prior years currently called (date_conf_2015_2016_2017.tif) is present. If it's not, it will download the raw 2015 / 2016 / 2017 data from S3 (s3://gfw2-data/forest_change/umd_landsat_alerts/archive/tiles/) and build it.

After building this old years raster, it will be copied to the `pre_processed` directory to be used in future updates. The same goes for dataset that we're updating currently- the 2018 date and conf we just added will be combined and saved in that `pre_processed` dir. That way, once we stop updating 2018 data (likely in July 2019), we'll use that saved copy of the TIF to build our all-years GLAD raster (2015 - 2019).

##### Intermediate update
To ensure that we archive the data properly, I'd recommend uploading 2018 date and confidence rasters (the raw data from UMD) to that same S3 location every time we update them. This isn't about keeping an archive really, more about establishing this process so that when we do stop updating 2018 data, and just in case all of our preprocessed 2018 data on the TERRANLYSIS server gets deleted, we'll have a backup.

This should be pretty easy to build into the existing workflow - after we download the data from UMD / google storage, we'll want to copy each confidence and date raster (per tile) here:
s3://gfw2-data/forest_change/umd_landsat_alerts/archive/tiles/{{tile_name}}/{{day|conf}}_2018.tif

##### When 2019 data becomes available

When 2019 data becomes available, the mapnik-forest-change code should work as expected. We've updated multiple years at once before without any issue.

The additional postprocessing code hasn't run for multiple years yet. Once 2019 data becomes available, we'll need to update our workflow to account for that. Right now, this will likely meaning `cat`ing 2018 and 2019 CSVs for each region (south_america, afr_asia) together before we run `tippecanoe` to create our vector tiles. I'd recommend doing a full review of the `update_glad_data.py` code in this repo to make sure there's nothing else required for this update as well.
