# ESF-OpenFrontier
A collaboration between Open Science Computing and Frontier Geospatial, hence OpenFrontier

## Creating COGs
* create a VRT from 1000x1000 pixel geotiff files for a single year using a python notebook
* convert the VRT to COG and push to S3

Ran the notebook to create the VRTs locally.


```
(pangeo) rsignell@OSC:~$ export AWS_ACCESS_KEY_ID=AKIAUxxxxxxxxx
(pangeo) rsignell@OSC:~$ export AWS_SECRET_ACCESS_KEY=Wkdzpxxxxxxxxxxxxx
(pangeo) rsignell@OSC:~$ export AWS_REGION=us-east-1
(pangeo) rsignell@OSC:~$ export AWS_VIRTUAL_HOSTING=yes
(pangeo) rsignell@OSC:~$ export AWS_HTTPS=yes
(pangeo) rsignell@OSC:~$ export AWS_DEFAULT_REGION=us-east-1
```
Can warp the VRT to Web Mercator projection using:
```
(pangeo) gdalwarp  agb_1990.vrt -s_srs epsg:5070 -t_srs epsg:3857 agb_1990_web.tif
```
Convert to COG using `rio cogeo`:
```
(pangeo) rsignell@OSC:~$ rio cogeo create agb_1990.vrt agb_1990_cog.tif
```
Or convert the warped TIF to a COG using:
```
(pangeo) rsignell@OSC:~$ rio cogeo create agb_1990_web.tif agb_1990_cog_geo.tif

```

Titiler viewer endpoints:
* [USGS titiler](https://titiler.usgs.gov/cog/viewer) 
* [XYZ titiler](https://titiler.xyz.com/cog/viewer)

The COGs must be public for titiler, so I moved one to my Open Storage Network s3-compatible endpoint.   So try dropping this COG URL in the Titiler endpoint above:

 https://usgs.osn.mghpcc.org/esip/rsignell/testing/agb_1990_cog.tif

