# ESF-OpenFrontier
A collaboration between Open Science Computing and Frontier Geospatial, hence OpenFrontier

## Creating COGs
* create a VRT from 1000x1000 pixel geotiff files for a single year using a python notebook
* convert the VRT to COG and push to S3

Ran the notebook to create the VRTs locally.
Converted the VRT to a COG using:
```
(pangeo) rsignell@OSC:~$ export AWS_ACCESS_KEY_ID=AKIAUxxxxxxxxx
(pangeo) rsignell@OSC:~$ export AWS_SECRET_ACCESS_KEY=Wkdzpxxxxxxxxxxxxx
(pangeo) rsignell@OSC:~$ export AWS_REGION=us-east-1
(pangeo) rsignell@OSC:~$ export AWS_VIRTUAL_HOSTING=yes
(pangeo) rsignell@OSC:~$ export AWS_HTTPS=yes
(pangeo) rsignell@OSC:~$ export AWS_DEFAULT_REGION=us-east-1
(pangeo) rsignell@OSC:~$ rio cogeo create agb_1990.vrt agb_1990_cog.tif
Reading input: /home/rsignell/agb_1990.vrt
  [####################################]  100%
```
Can warp to another projection using:
```
 gdalwarp agb_1990_cog.tif -s_srs epsg:5070 -t_srs epsg:3857 agb_1990_web.tif
```
https://titiler.usgs.gov/cog/viewer
https://titiler.xyz.com/cog/viewer
```
https://usgs.osn.mghpcc.org/esip/rsignell/testing/agb_1990_cog.tif
https://opendata.digitalglobe.com/events/mauritius-oil-spill/post-event/2020-08-12/105001001F1B5B00/105001001F1B5B00.tif
```
