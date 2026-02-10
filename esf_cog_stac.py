"""
Build COGs and STAC Catalog for ESF AGB Data.

Converts ESF Landsat ensemble AGB data from per-year S3 tiles into:
1. One COG per year — uploaded to publicly accessible Cloudflare R2 storage
2. A static STAC catalog — so tools can discover and load the data

Each year is processed in parallel on a separate Coiled Dask worker.

Usage:

    export ESF_AWS_ACCESS_KEY_ID=AKIA...
    export ESF_AWS_SECRET_ACCESS_KEY=...
    export R2_ACCESS_KEY_ID=...
    export R2_SECRET_ACCESS_KEY=...
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
import rasterio
from dask.distributed import Client, as_completed
from osgeo import gdal
from rasterio.crs import CRS
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles

# ---------- Configuration ----------

YEARS = range(1990, 2024)
SOURCE_BUCKET = 'cafri-share'
SOURCE_PREFIX = 'landsat_ensemble_agb_2.0.0'

R2_ENDPOINT = 'https://9cbdcb4884f86a6779032ae561e474a5.r2.cloudflarestorage.com'
R2_BUCKET = 'osc'
R2_PREFIX = 'esf-agb'
PUBLIC_BASE_URL = f'https://pub-59649a08584b41c490cb84732702591a.r2.dev/{R2_PREFIX}'

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
    'R2_ACCESS_KEY_ID',
    'R2_SECRET_ACCESS_KEY',
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

        # Fix CRS and embed statistics for titiler auto-scaling
        with rasterio.open(str(cog_path), 'r+',
                           IGNORE_COG_LAYOUT_BREAK='YES') as dst:
            dst.crs = CRS.from_epsg(5070)
            data = dst.read(1)
            valid = data[data != -9999]
            dst.update_tags(1,
                STATISTICS_MINIMUM=f'{valid.min():.2f}',
                STATISTICS_MAXIMUM=f'{valid.max():.2f}',
                STATISTICS_MEAN=f'{valid.mean():.2f}',
                STATISTICS_STDDEV=f'{valid.std():.2f}',
            )

        size_mb = cog_path.stat().st_size / 1e6

        # --- Upload to R2 ---
        import boto3
        r2_client = boto3.client(
            's3',
            endpoint_url=R2_ENDPOINT,
            aws_access_key_id=os.environ['R2_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['R2_SECRET_ACCESS_KEY'],
            region_name='auto',
        )
        s3_key = f'{R2_PREFIX}/agb_{year}_cog.tif'
        r2_client.upload_file(str(cog_path), R2_BUCKET, s3_key)

    elapsed = time.time() - t0
    return year, size_mb, elapsed


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

    # Build STAC catalog locally (saved to repo root for GitHub Pages)
    print('Building STAC catalog...')
    from build_stac import build_stac as build_stac_local
    build_stac_local()

    client.close()
    cluster.close()
    print('\nDone!')


if __name__ == '__main__':
    main()
