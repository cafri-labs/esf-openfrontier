"""
Create a static STAC catalog for the ESF AGB COGs on OSN.
Saves catalog.json + collection/item JSONs to the repo root
for hosting via GitHub Pages.
"""

from datetime import datetime, timezone
from pathlib import Path

import pystac
import rasterio
from pyproj import Transformer
from shapely.geometry import box, mapping

# ---------- Configuration ----------
YEARS = range(1990, 2024)
OSN_ENDPOINT = 'https://usgs.osn.mghpcc.org'
OSN_BUCKET = 'esip'
OSN_PREFIX = 'rsignell/esf'
PUBLIC_BASE_URL = f'{OSN_ENDPOINT}/{OSN_BUCKET}/{OSN_PREFIX}'

# Get spatial extent and projection info from one COG (all years share the same grid)
sample_url = f'{PUBLIC_BASE_URL}/agb_1992_cog.tif'
print(f'Reading metadata from {sample_url}...')

with rasterio.open(sample_url) as src:
    native_bounds = src.bounds
    crs = src.crs
    native_transform = list(src.transform)[:6]
    native_shape = [src.height, src.width]

# Transform bounds to EPSG:4326 for STAC
transformer = Transformer.from_crs(crs, 'EPSG:4326', always_xy=True)
west, south = transformer.transform(native_bounds.left, native_bounds.bottom)
east, north = transformer.transform(native_bounds.right, native_bounds.top)
bbox_4326 = [west, south, east, north]
geometry_4326 = mapping(box(*bbox_4326))
proj_bbox = [native_bounds.left, native_bounds.bottom, native_bounds.right, native_bounds.top]

print(f'CRS:        {crs}')
print(f'Shape:      {native_shape}')
print(f'STAC bbox:  {[round(c, 4) for c in bbox_4326]}')

# Create the STAC Collection
spatial_extent = pystac.SpatialExtent(bboxes=[bbox_4326])
temporal_extent = pystac.TemporalExtent(
    intervals=[[datetime(1990, 1, 1, tzinfo=timezone.utc),
                datetime(2023, 1, 1, tzinfo=timezone.utc)]]
)

collection = pystac.Collection(
    id='esf-agb',
    title='ESF Landsat Ensemble Aboveground Biomass',
    description=(
        'Annual aboveground biomass (AGB) carbon estimates (Mg/ha) for the '
        'eastern United States, derived from Landsat ensemble models. '
        '30 m resolution, EPSG:5070. Version 2.0.0.'
    ),
    license='proprietary',
    extent=pystac.Extent(spatial=spatial_extent, temporal=temporal_extent),
    providers=[
        pystac.Provider(
            name='ESF / Frontier Geospatial',
            roles=[pystac.ProviderRole.PRODUCER],
        ),
    ],
)

# Add items — one per year
for year in YEARS:
    cog_url = f'{PUBLIC_BASE_URL}/agb_{year}_cog.tif'

    item = pystac.Item(
        id=f'agb-{year}',
        geometry=geometry_4326,
        bbox=bbox_4326,
        datetime=datetime(year, 1, 1, tzinfo=timezone.utc),
        properties={
            'proj:epsg': 5070,
            'proj:shape': native_shape,
            'proj:transform': native_transform,
            'proj:bbox': proj_bbox,
        },
    )
    item.add_asset(
        'data',
        pystac.Asset(
            href=cog_url,
            media_type=pystac.MediaType.COG,
            roles=['data'],
            title=f'AGB {year}',
        ),
    )
    collection.add_item(item)

print(f'Collection: {len(list(collection.get_items()))} items')

# Create root catalog
catalog = pystac.Catalog(
    id='esf-openfrontier',
    title='ESF OpenFrontier',
    description='Geospatial datasets from the ESF / Frontier Geospatial collaboration.',
)
catalog.add_child(collection)

# Save to repo root with relative links (works from any base URL including GitHub Pages)
repo_root = Path('.')
catalog.normalize_hrefs(str(repo_root))
catalog.validate_all()
print('STAC validates OK')
catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)

print('\nFiles written:')
print('  catalog.json')
for p in sorted(repo_root.rglob('esf-agb/**/*.json')):
    print(f'  {p}')

print(f'\nGitHub Pages URL: https://opensciencecomputing.github.io/esf-openfrontier/catalog.json')
