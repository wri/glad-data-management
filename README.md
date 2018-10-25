# glad-data-management
This repo brings together a few separate processes under one roof. In general, the order of operations is as follows:

### Kicking off the script
From the data management server, go to the gfw-sync2 directory and run:

`python gfw-sync.py -e prod -l umd_landsat_alerts`

This starts the terranalyis linux server, SSHes in, and runs `update_glad_data.py` (in this repo).

- Input: Currently manually triggered (usually Monday or Tuesday). Could certainly be scheduled using gfw-sync, but still some manual steps (bottom of this README)
- Output: Kicking off the main script

### Generating map tiles & rasters for dynamic analysis + visualization
The first thing update_glad_data.py does is run `generate_tiles.py` in the [mapnik-forest-change-tiles repo](https://github.com/wri/mapnik-forest-change-tiles/).

Map tiles:
- Input: new GLAD data downloaded from google storage exports (also available [here](http://glad-forest-alert.appspot.com)).
- Output: z/x/y map tiles on s3 / https://tiles.globalforestwatch.org/glad_prod/tiles/z/x/y.png

Rasters for [dynamic analysis](https://github.com/wri/glad-raster-analysis-lambda) of small polygons, and 
[visualization](https://github.com/gfw-api/lambda-tiler) of map data at zooms 10 - 12.
- Input: new GLAD data
- Output: 
  - TIFs for visualization: s3://palm-risk-poc/data/glad/rgb/ 
  - TIFs for flagship analysis: s3://palm-risk-poc/data/glad/analysis-staging/
  - TIFs for Pro analysis: s3://gfwpro-raster-data/

### GLAD vector data
The script then starts processing/utilities/weekly_update.py in the [raster-vector-to-tsv repo](https://github.com/wri/raster-vector-to-tsv/)

- Input: new GLAD data
- Output: GLAD points as CSV, with emissions and climate mask data as well: s3://gfw2-data/alerts-tsv/glad/

### Attach iso/adm1/adm2 
Following writing to point, it starts `update_country_stats.py` from the [gfw-country-pages-analysis-2 repo](https://github.com/wri/gfw-country-pages-analysis-2) in the background.
This spins up a hadoop cluster to attach iso/adm1/adm2 data to the points in s3://gfw2-data/alerts-tsv/glad/. 


It also updates the pre-calculated iso/adm1/adm2 stats stored in elastic. Currently these datasets are stored here:

##### Datasets for [charts](https://www.globalforestwatch.org/dashboards/country/BRA?widget=gladAlerts#gladAlerts) on country pages - grouped by week
- iso dataset: [391ca96d-303f-4aef-be4b-9cdb4856832c](https://production-api.globalforestwatch.org/dataset/391ca96d-303f-4aef-be4b-9cdb4856832c/)
- adm1 dataset: [c7a1d922-e320-4e92-8e4c-11ea33dd6e35](https://production-api.globalforestwatch.org/dataset/c7a1d922-e320-4e92-8e4c-11ea33dd6e35)
- adm2 dataset: [428db321-5ebb-4e86-a3df-32c63b6d3c83](https://production-api.globalforestwatch.org/dataset/428db321-5ebb-4e86-a3df-32c63b6d3c83)

##### Dataset for [climate insights](http://climate.globalforestwatch.org/insights/glad-alerts/BRA) - grouped by week
- iso/adm1/adm2: [a98197d2-cd8e-4b17-ab5c-fabf54b25ea0](https://production-api.globalforestwatch.org/dataset/a98197d2-cd8e-4b17-ab5c-fabf54b25ea0)

##### Dataset for glad-alerts/admin/ endpoint- sums alerts for iso/adm1/adm2, grouped by day
- iso/adm1/adm2: [63e88e53-0a88-416e-9532-fa06f703d435](http://production-api.globalforestwatch.org/dataset/63e88e53-0a88-416e-9532-fa06f703d435)

These dataset IDs may change, please check the gfw-country-pages [config sheet](https://docs.google.com/spreadsheets/d/174wtlPMWENa1FCYXHqzwvZB5vi7DjLwX-oQjaUEdxzo/edit#gid=923735044) to be sure these IDs are up to date.

- Input: GLAD CSVs, iso/adm1/adm2 boundaries
- Output: updated iso/adm1/adm2 summary datasets

### Update the sqlite database used for custom large polygons in glad-alerts-tiled MS
In an attempt to pre-calculate as much as we can for large user-drawn AOIs, we tabulate the count of GLAD alerts by day
within each z/x/y map tile. This data is stored here: s3://palm-risk-poc/data/mvt/stats.db. Whenever a user submits a large 
custom polygon, our glad-alerts MS (code [here](https://github.com/gfw-api/glad-analysis-tiled)) tiles that geometry into 
z/x/y tiles, then queries the database to estimate the count of alerts.

To update this database, we convert the updated CSVs to vector tiles, then to z/x/y/date summaries.

- Input: GLAD CSVs
- Output: tiled z/x/y summary stats

### Write iso/adm1/adm2 download CSVs
Users also want to download GLAD extracts by country. We pre-generate these using the `split_glad.py` code in this repo.

- Input: GLAD CSV from hadoop with iso/adm1/adm2 attached
- Output: CSV extracts here: s3://gfw2-data/alerts-tsv/glad-download/

### Deploying the updated MS
This is currently a manual step- should be automated in the future. To ensure that our 
[glad-analysis-tiled](https://github.com/gfw-api/glad-analysis-tiled) MS is using the latest data, we need to go in
to jenkins and build + deploy this container on PROD.

After this has done, check the [/latest endpoint](https://production-api.globalforestwatch.org/v1/glad-alerts/latest) to be sure
it matches the expected max date in the data.

When this is all set, POST with a blank body to `https://production-api.globalforestwatch.org/subscriptions/notify-updates/glad-alerts`
to kick off the subscription update process.

### Summary

Wow that was fun! So many things! GLAD alerts are pretty pervasive throughout the platform- visually as map tiles, used 
for custom and country-level analysis, aggregated for display on charts, and available to download as CSV. The size and 
update frequency of this dataset means a one-size-fits-all solution is currently not possible. Hopefully data processing can be 
more unified in the future.
