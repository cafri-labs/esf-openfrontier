"""
Split NYS_parcels.gpkg into per-county FlatGeobuf files for R2 upload.

- Keeps only fields used in the parcels.html viewer
- Replaces PROP_CLASS code with human-readable label from ny_property_class_codes.csv
- Reprojects to EPSG:4326 (WGS84) for web use
- Outputs to parcels/ directory, one .fgb per county

Usage:
    conda activate esf
    python split_parcels.py
"""

import re
from pathlib import Path

import geopandas as gpd
import pandas as pd

GPKG = Path('NYS_parcels.gpkg')
CSV  = Path('ny_property_class_codes.csv')
OUT  = Path('parcels')

KEEP_FIELDS = ['SWIS_SBL_ID', 'COUNTY_NAME', 'PROP_CLASS', 'live_c_delta_90_23_sum']

OUT.mkdir(exist_ok=True)

print('Loading property class codes...')
codes = pd.read_csv(CSV, dtype={'code': str})
code_map = dict(zip(codes['code'], codes['description']))

print(f'Reading {GPKG} ...')
gdf = gpd.read_file(GPKG)
print(f'  {len(gdf):,} features, CRS: {gdf.crs}')

# Keep only needed fields
missing = [f for f in KEEP_FIELDS if f not in gdf.columns]
if missing:
    raise ValueError(f'Missing expected fields: {missing}')
gdf = gdf[KEEP_FIELDS + ['geometry']].copy()

# Calculate acres from geometry (reproject to equal-area for accuracy)
gdf['acres'] = gdf.to_crs(epsg=5070).geometry.area / 4046.856

# Replace PROP_CLASS code with label
gdf['PROP_CLASS'] = gdf['PROP_CLASS'].astype(str).map(code_map).fillna(gdf['PROP_CLASS'].astype(str))

# Reproject to WGS84
if gdf.crs and gdf.crs.to_epsg() != 4326:
    print(f'  Reprojecting from {gdf.crs} to EPSG:4326...')
    gdf = gdf.to_crs(epsg=4326)

FILTER_COUNTIES = None  # set to a list like ['Greene', 'Essex'] to limit processing

counties = gdf['COUNTY_NAME'].dropna().unique()
if FILTER_COUNTIES:
    counties = [c for c in counties if c in FILTER_COUNTIES]
print(f'  {len(counties)} counties to process')

for county in sorted(counties):
    subset = gdf[gdf['COUNTY_NAME'] == county].copy()
    slug = re.sub(r'[^a-z0-9]+', '_', county.lower()).strip('_')
    out_path = OUT / f'{slug}.fgb'
    subset.to_file(out_path, driver='FlatGeobuf')
    print(f'  {county}: {len(subset):,} features → {out_path}')

print(f'\nDone. {len(counties)} files written to {OUT}/')
