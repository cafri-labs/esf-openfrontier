"""
Build COGs and STAC Catalog for ESF AGB Data.

Converts ESF Landsat ensemble AGB data from per-year S3 tiles into:
1. One COG per year — uploaded to publicly accessible OSN storage
2. A static STAC catalog — so tools can discover and load the data

Usage with coiled batch:

    # First, set your secrets (one time):
    coiled secret create ESF_AWS_ACCESS_KEY_ID --value "AKIA..."
    coiled secret create ESF_AWS_SECRET_ACCESS_KEY --value "..."
    coiled secret create OSN_AWS_ACCESS_KEY_ID --value "..."
    coiled secret create OSN_AWS_SECRET_ACCESS_KEY --value "..."

    # Then run:
    coiled batch run \\
        --region us-east-1 \\
        --software esf \\
        --secret-env ESF_AWS_ACCESS_KEY_ID \\
        --secret-env ESF_AWS_SECRET_ACCESS_KEY \\
        --secret-env OSN_AWS_ACCESS_KEY_ID \\
        --secret-env OSN_AWS_SECRET_ACCESS_KEY \\
        -- python esf_cog_stac.py
"""

import os
import time
from datetime import datetime, timezone
from pathlib import Path

import fsspec
import pystac
import rasterio
from osgeo import gdal
from pyproj import Transformer
from rasterio.crs import CRS
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles
from shapely.geometry import box, mapping

# ---------- Configuration ----------

YEARS = range(1990, 2024)
SOURCE_BUCKET = 'cafri-share'
SOURCE_PREFIX = 'landsat_ensemble_agb_2.0.0'

OSN_ENDPOINT = 'https://usgs.osn.mghpcc.org'
OSN_BUCKET = 'esip'
OSN_PREFIX = 'rsignell/esf-agb'
PUBLIC_BASE_URL = f'{OSN_ENDPOINT}/{OSN_BUCKET}/{OSN_PREFIX}'

LOCAL_COG_DIR = Path('cogs')
LOCAL_VRT_DIR = Path('vrts')
LOCAL_STAC_DIR = Path('stac')


# ---------- Credentials ----------

def get_read_credentials():
    """Get credentials for reading from cafri-share (standard AWS S3)."""
    key = os.environ.get('ESF_AWS_ACCESS_KEY_ID')
    secret = os.environ.get('ESF_AWS_SECRET_ACCESS_KEY')
    if key and secret:
        return key, secret
    raise RuntimeError(
        'Missing ESF credentials. Set ESF_AWS_ACCESS_KEY_ID and '
        'ESF_AWS_SECRET_ACCESS_KEY environment variables.'
    )


def get_write_credentials():
    """Get credentials for writing to OSN."""
    key = os.environ.get('OSN_AWS_ACCESS_KEY_ID')
    secret = os.environ.get('OSN_AWS_SECRET_ACCESS_KEY')
    if key and secret:
        return key, secret
    raise RuntimeError(
        'Missing OSN credentials. Set OSN_AWS_ACCESS_KEY_ID and '
        'OSN_AWS_SECRET_ACCESS_KEY environment variables.'
    )


def setup_gdal_for_read():
    """Configure GDAL to read from cafri-share via S3."""
    key, secret = get_read_credentials()
    os.environ['AWS_ACCESS_KEY_ID'] = key
    os.environ['AWS_SECRET_ACCESS_KEY'] = secret
    os.environ.pop('AWS_SESSION_TOKEN', None)
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

    gdal.SetConfigOption('GDAL_DISABLE_READDIR_ON_OPEN', 'EMPTY_DIR')
    gdal.SetConfigOption('AWS_VIRTUAL_HOSTING', 'YES')

    # Required for S3-compatible stores like OSN (Ceph-based)
    os.environ['AWS_REQUEST_CHECKSUM_CALCULATION'] = 'WHEN_REQUIRED'
    os.environ['AWS_RESPONSE_CHECKSUM_VALIDATION'] = 'WHEN_REQUIRED'


def get_read_fs():
    """Get an fsspec S3 filesystem for reading source tiles."""
    key, secret = get_read_credentials()
    return fsspec.filesystem('s3', key=key, secret=secret)


def get_write_fs():
    """Get an fsspec S3 filesystem for writing to OSN."""
    key, secret = get_write_credentials()
    return fsspec.filesystem(
        's3', key=key, secret=secret,
        client_kwargs={'endpoint_url': OSN_ENDPOINT},
    )


# ---------- Step 1: Build VRTs ----------

def build_vrt(year, fs_read):
    """List S3 tiles for a year and build a VRT."""
    LOCAL_VRT_DIR.mkdir(exist_ok=True)
    vrt_path = LOCAL_VRT_DIR / f'agb_{year}.vrt'

    tile_keys = fs_read.glob(
        f's3://{SOURCE_BUCKET}/{SOURCE_PREFIX}_{year}/*.tiff'
    )
    vsi_paths = [f'/vsis3/{key}' for key in tile_keys]

    if not vsi_paths:
        raise RuntimeError(f'No tiles found for year {year}')

    vrt_options = gdal.BuildVRTOptions(
        resolution='highest', resampleAlg='nearest'
    )
    vrt_ds = gdal.BuildVRT(str(vrt_path), vsi_paths, options=vrt_options)
    vrt_ds = None  # flush to disk

    return vrt_path


# ---------- Step 2: VRT → COG ----------

def create_cog(vrt_path, cog_path):
    """Convert a VRT to a Cloud Optimized GeoTIFF with proper EPSG:5070 CRS."""
    if cog_path.exists():
        cog_path.unlink()

    output_profile = cog_profiles.get('deflate')
    cog_translate(
        str(vrt_path),
        str(cog_path),
        output_profile,
        overview_resampling='average',
        use_cog_driver=True,
    )
    # Source tiles use a custom WKT (Albers + WGS84 datum) that doesn't match
    # EPSG:5070 (Albers + NAD83). Parameters are identical, so we reassign
    # without reprojecting. Fixes odc-stac / WarpedVRT issues.
    with rasterio.open(str(cog_path), 'r+',
                       IGNORE_COG_LAYOUT_BREAK='YES') as dst:
        dst.crs = CRS.from_epsg(5070)

    return cog_path


# ---------- Step 3: Upload to OSN ----------

def upload_to_osn(osn_fs, local_path, s3_key):
    """Upload a file to OSN using fsspec."""
    osn_fs.put(str(local_path), f'{OSN_BUCKET}/{s3_key}')


# ---------- Step 4: Build STAC catalog ----------

def build_stac(osn_fs):
    """Build a STAC catalog from the uploaded COGs and upload to OSN."""
    sample_url = f'{PUBLIC_BASE_URL}/agb_{YEARS[0]}_cog.tif'
    with rasterio.open(sample_url) as src:
        native_bounds = src.bounds
        crs = src.crs
        native_transform = list(src.transform)[:6]
        native_shape = [src.height, src.width]

    transformer = Transformer.from_crs(crs, 'EPSG:4326', always_xy=True)
    west, south = transformer.transform(native_bounds.left, native_bounds.bottom)
    east, north = transformer.transform(native_bounds.right, native_bounds.top)
    bbox_4326 = [west, south, east, north]
    geometry_4326 = mapping(box(*bbox_4326))
    proj_bbox = [native_bounds.left, native_bounds.bottom,
                 native_bounds.right, native_bounds.top]

    collection = pystac.Collection(
        id='esf-agb',
        title='ESF Landsat Ensemble Aboveground Biomass',
        description=(
            'Annual aboveground biomass (AGB) carbon estimates (Mg/ha) for the '
            'eastern United States, derived from Landsat ensemble models. '
            '30 m resolution, EPSG:5070. Version 2.0.0.'
        ),
        license='proprietary',
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent(bboxes=[bbox_4326]),
            temporal=pystac.TemporalExtent(
                intervals=[[datetime(YEARS[0], 1, 1, tzinfo=timezone.utc),
                             datetime(YEARS[-1], 1, 1, tzinfo=timezone.utc)]]
            ),
        ),
        providers=[
            pystac.Provider(
                name='ESF / Frontier Geospatial',
                roles=[pystac.ProviderRole.PRODUCER],
            ),
        ],
    )

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

    catalog = pystac.Catalog(
        id='esf-openfrontier',
        title='ESF OpenFrontier',
        description='Geospatial datasets from the ESF / Frontier Geospatial collaboration.',
    )
    catalog.add_child(collection)
    catalog.normalize_hrefs(f'{PUBLIC_BASE_URL}/stac')
    catalog.validate_all()

    LOCAL_STAC_DIR.mkdir(exist_ok=True)
    catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED,
                 dest_href=str(LOCAL_STAC_DIR))

    for json_path in LOCAL_STAC_DIR.rglob('*.json'):
        rel_path = json_path.relative_to(LOCAL_STAC_DIR)
        s3_key = f'{OSN_PREFIX}/stac/{rel_path}'
        osn_fs.put(str(json_path), f'{OSN_BUCKET}/{s3_key}')
        print(f'  uploaded {s3_key}')

    print(f'\nSTAC catalog URL: {PUBLIC_BASE_URL}/stac/catalog.json')


# ---------- Main ----------

def main():
    print(f'Processing {len(YEARS)} years: {YEARS[0]}–{YEARS[-1]}')
    print(f'Output: {PUBLIC_BASE_URL}/\n')

    setup_gdal_for_read()
    fs_read = get_read_fs()
    osn_fs = get_write_fs()

    LOCAL_COG_DIR.mkdir(exist_ok=True)

    for year in YEARS:
        t0 = time.time()

        print(f'  {year}: building VRT...', end='', flush=True)
        vrt_path = build_vrt(year, fs_read)
        print(f' creating COG...', end='', flush=True)

        cog_path = LOCAL_COG_DIR / f'agb_{year}_cog.tif'
        create_cog(vrt_path, cog_path)
        size_mb = cog_path.stat().st_size / 1e6
        print(f' {size_mb:.0f} MB → uploading...', end='', flush=True)

        s3_key = f'{OSN_PREFIX}/agb_{year}_cog.tif'
        upload_to_osn(osn_fs, cog_path, s3_key)

        elapsed = time.time() - t0
        print(f' done ({elapsed:.0f}s)')

        cog_path.unlink()
        vrt_path.unlink()

    print('\nAll COGs uploaded.\n')

    print('Building STAC catalog...')
    build_stac(osn_fs)

    print('\nDone!')


if __name__ == '__main__':
    main()
