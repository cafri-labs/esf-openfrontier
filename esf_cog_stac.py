"""
Build COGs and STAC Catalog for ESF AGB Data.

Converts ESF Landsat ensemble AGB data from per-year S3 tiles into:
1. One COG per year — uploaded to publicly accessible OSN storage
2. A static STAC catalog — so tools can discover and load the data

Each year is processed in parallel on a separate Coiled Dask worker.

Usage:

    export ESF_AWS_ACCESS_KEY_ID=AKIA...
    export ESF_AWS_SECRET_ACCESS_KEY=...
    export OSN_AWS_ACCESS_KEY_ID=...
    export OSN_AWS_SECRET_ACCESS_KEY=...
    python esf_cog_stac.py

    Or put the variables in a secrets.env file (without "export") and source it:

        source secrets.env
        python esf_cog_stac.py
"""

import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import coiled
import fsspec
import numpy as np
import pystac
import rasterio
from dask.distributed import Client, as_completed
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
OSN_PREFIX = 'rsignell/esf-osc'
PUBLIC_BASE_URL = f'{OSN_ENDPOINT}/{OSN_BUCKET}/{OSN_PREFIX}'

# Coiled cluster settings
N_WORKERS = 34
WORKER_MEMORY = '16 GiB'
COILED_SOFTWARE = 'esf'
COILED_WORKSPACE = 'osc-aws'
COILED_REGION = 'us-east-1'

# Credentials env var names (forwarded to workers)
CRED_ENV_VARS = [
    'ESF_AWS_ACCESS_KEY_ID',
    'ESF_AWS_SECRET_ACCESS_KEY',
    'OSN_AWS_ACCESS_KEY_ID',
    'OSN_AWS_SECRET_ACCESS_KEY',
]


# ---------- Worker function ----------

def process_year(year):
    """Process a single year on a Dask worker: VRT → COG → upload to OSN."""
    import os
    import tempfile
    import time
    from pathlib import Path

    import fsspec
    import numpy as np
    import rasterio
    from osgeo import gdal
    from rasterio.crs import CRS
    from rio_cogeo.cogeo import cog_translate
    from rio_cogeo.profiles import cog_profiles

    t0 = time.time()

    # Configure GDAL for S3 reads on this worker
    esf_key = os.environ['ESF_AWS_ACCESS_KEY_ID']
    esf_secret = os.environ['ESF_AWS_SECRET_ACCESS_KEY']
    os.environ['AWS_ACCESS_KEY_ID'] = esf_key
    os.environ['AWS_SECRET_ACCESS_KEY'] = esf_secret
    os.environ.pop('AWS_SESSION_TOKEN', None)
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
    os.environ['AWS_REQUEST_CHECKSUM_CALCULATION'] = 'WHEN_REQUIRED'
    os.environ['AWS_RESPONSE_CHECKSUM_VALIDATION'] = 'WHEN_REQUIRED'

    gdal.SetConfigOption('GDAL_DISABLE_READDIR_ON_OPEN', 'EMPTY_DIR')
    gdal.SetConfigOption('AWS_VIRTUAL_HOSTING', 'YES')

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # --- Build VRT ---
        fs_read = fsspec.filesystem('s3', key=esf_key, secret=esf_secret)
        tile_keys = fs_read.glob(
            f's3://{SOURCE_BUCKET}/{SOURCE_PREFIX}_{year}/*.tiff'
        )
        vsi_paths = [f'/vsis3/{key}' for key in tile_keys]

        if not vsi_paths:
            raise RuntimeError(f'No tiles found for year {year}')

        vrt_path = tmpdir / f'agb_{year}.vrt'
        vrt_options = gdal.BuildVRTOptions(
            resolution='highest', resampleAlg='nearest'
        )
        vrt_ds = gdal.BuildVRT(str(vrt_path), vsi_paths, options=vrt_options)
        vrt_ds = None  # flush to disk

        # --- Create COG with NaN → -9999 ---
        tmp_tif = tmpdir / f'agb_{year}.tmp.tif'
        cog_path = tmpdir / f'agb_{year}_cog.tif'

        with rasterio.open(str(vrt_path)) as src:
            profile = src.profile.copy()
            profile.update(driver='GTiff', nodata=-9999)
            with rasterio.open(str(tmp_tif), 'w', **profile) as dst:
                for i in range(1, src.count + 1):
                    data = src.read(i)
                    data = np.where(np.isnan(data), -9999, data)
                    dst.write(data, i)

        output_profile = cog_profiles.get('deflate')
        output_profile['nodata'] = -9999
        cog_translate(
            str(tmp_tif),
            str(cog_path),
            output_profile,
            overview_resampling='average',
            use_cog_driver=True,
        )
        tmp_tif.unlink()

        # Fix CRS: custom WKT (Albers + WGS84) → proper EPSG:5070 (Albers + NAD83)
        with rasterio.open(str(cog_path), 'r+',
                           IGNORE_COG_LAYOUT_BREAK='YES') as dst:
            dst.crs = CRS.from_epsg(5070)

        size_mb = cog_path.stat().st_size / 1e6

        # --- Upload to OSN ---
        osn_key = os.environ['OSN_AWS_ACCESS_KEY_ID']
        osn_secret = os.environ['OSN_AWS_SECRET_ACCESS_KEY']
        osn_fs = fsspec.filesystem(
            's3', key=osn_key, secret=osn_secret,
            client_kwargs={'endpoint_url': OSN_ENDPOINT},
        )
        s3_key = f'{OSN_PREFIX}/agb_{year}_cog.tif'
        osn_fs.put(str(cog_path), f'{OSN_BUCKET}/{s3_key}')

    elapsed = time.time() - t0
    return year, size_mb, elapsed


# ---------- STAC catalog (runs locally after cluster work) ----------

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

    stac_dir = Path('stac')
    stac_dir.mkdir(exist_ok=True)
    catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED,
                 dest_href=str(stac_dir))

    for json_path in stac_dir.rglob('*.json'):
        rel_path = json_path.relative_to(stac_dir)
        s3_key = f'{OSN_PREFIX}/stac/{rel_path}'
        osn_fs.put(str(json_path), f'{OSN_BUCKET}/{s3_key}')
        print(f'  uploaded {s3_key}')

    print(f'\nSTAC catalog URL: {PUBLIC_BASE_URL}/stac/catalog.json')


# ---------- Main ----------

def main():
    years = list(YEARS)
    print(f'Processing {len(years)} years: {years[0]}–{years[-1]}')
    print(f'Output: {PUBLIC_BASE_URL}/\n')

    # Verify credentials are available locally before spinning up a cluster
    for var in CRED_ENV_VARS:
        if not os.environ.get(var):
            raise RuntimeError(f'Missing environment variable: {var}')

    # Forward credentials to workers via environ
    worker_env = {var: os.environ[var] for var in CRED_ENV_VARS}

    print(f'Starting Coiled cluster ({N_WORKERS} workers, {WORKER_MEMORY} each)...')
    cluster = coiled.Cluster(
        n_workers=N_WORKERS,
        worker_memory=WORKER_MEMORY,
        workspace=COILED_WORKSPACE,
        software=COILED_SOFTWARE,
        region=COILED_REGION,
        environ=worker_env,
    )
    client = Client(cluster)
    print(f'Dashboard: {client.dashboard_link}\n')

    # Map one year per worker
    futures = client.map(process_year, years)

    completed = 0
    for future in as_completed(futures):
        year, size_mb, elapsed = future.result()
        completed += 1
        print(f'  [{completed}/{len(years)}] {year}: {size_mb:.0f} MB ({elapsed:.0f}s)')

    print('\nAll COGs uploaded.\n')

    # Build and upload STAC catalog (runs locally, reads from public URLs)
    print('Building STAC catalog...')
    osn_fs = fsspec.filesystem(
        's3',
        key=os.environ['OSN_AWS_ACCESS_KEY_ID'],
        secret=os.environ['OSN_AWS_SECRET_ACCESS_KEY'],
        client_kwargs={'endpoint_url': OSN_ENDPOINT},
    )
    build_stac(osn_fs)

    client.close()
    cluster.close()
    print('\nDone!')


if __name__ == '__main__':
    main()
