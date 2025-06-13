# ESF-OpenFrontier
A collaboration between Open Science Computing and Frontier Geospatial, hence OpenFrontier

## Converting from 1000x1000 pixel geotiff files to a single COG

```
 gdalwarp agb_1990_cog.tif -s_srs epsg:5070 -t_srs epsg:3857 agb_1990_web.tif
```
https://titiler.usgs.gov/cog/viewer
```
https://usgs.osn.mghpcc.org/rsignell/testing/agb_1990_web.tif
```
