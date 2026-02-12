"""
Build COGs for ESF Landsat Ensemble Data.

Converts ESF Landsat ensemble data from per-year S3 tiles into
COGs uploaded to Cloudflare R2.

Each (variable, year) pair is processed in parallel on a Coiled Dask worker.

Usage:

    export ESF_AWS_ACCESS_KEY_ID=AKIA...
    export ESF_AWS_SECRET_ACCESS_KEY=...
    export R2_ACCESS_KEY_ID=...
    export R2_SECRET_ACCESS_KEY=...
    python esf_cog_stac.py
"""

import os
import tempfile
import time
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

# VARIABLES = ['agb', 'agc', 'bgc']
VARIABLES = ['agc', 'bgc']
YEARS = range(1990, 2024)
SOURCE_BUCKET = 'cafri-share'
SOURCE_VERSION = '2.0.0'

R2_ENDPOINT = 'https://9cbdcb4884f86a6779032ae561e474a5.r2.cloudflarestorage.com'
R2_BUCKET = 'osc'

# Coiled cluster settings
N_WORKERS = 17
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

def process_year(args):
    """Process a single (variable, year) on a Dask worker: VRT -> COG -> upload."""
    variable, year = args

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

    source_bucket = 'cafri-share'
    source_prefix = f'landsat_ensemble_{variable}_2.0.0'
    r2_endpoint = 'https://9cbdcb4884f86a6779032ae561e474a5.r2.cloudflarestorage.com'
    r2_bucket = 'osc'
    r2_prefix = f'esf-{variable}'

    # Configure GDAL for S3 reads on this worker
    esf_key = os.environ['ESF_AWS_ACCESS_KEY_ID']
    esf_secret = os.environ['ESF_AWS_SECRET_ACCESS_KEY']

    # Set credentials via GDAL config options (more reliable than env vars)
    gdal.SetConfigOption('AWS_ACCESS_KEY_ID', esf_key)
    gdal.SetConfigOption('AWS_SECRET_ACCESS_KEY', esf_secret)
    gdal.SetConfigOption('AWS_DEFAULT_REGION', 'us-east-1')
    gdal.SetConfigOption('GDAL_DISABLE_READDIR_ON_OPEN', 'EMPTY_DIR')
    gdal.SetConfigOption('AWS_VIRTUAL_HOSTING', 'YES')

    os.environ['AWS_REQUEST_CHECKSUM_CALCULATION'] = 'WHEN_REQUIRED'
    os.environ['AWS_RESPONSE_CHECKSUM_VALIDATION'] = 'WHEN_REQUIRED'

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # --- Build VRT ---
        fs_read = fsspec.filesystem('s3', key=esf_key, secret=esf_secret)
        tile_keys = fs_read.glob(
            f's3://{source_bucket}/{source_prefix}_{year}/*.tiff'
        )
        vsi_paths = [f'/vsis3/{key}' for key in tile_keys]

        if not vsi_paths:
            raise RuntimeError(f'No tiles found for {variable} {year}')

        vrt_path = tmpdir / f'{variable}_{year}.vrt'

        # Verify GDAL can open a tile before building VRT
        gdal.UseExceptions()
        test_ds = gdal.Open(vsi_paths[0])
        if test_ds is None:
            err = gdal.GetLastErrorMsg()
            raise RuntimeError(
                f'Cannot open tile {vsi_paths[0]}: {err}'
            )
        test_info = f'{test_ds.RasterXSize}x{test_ds.RasterYSize}, {test_ds.RasterCount} band(s)'
        test_ds = None

        vrt_options = gdal.BuildVRTOptions(
            resolution='highest', resampleAlg='nearest'
        )
        vrt_ds = gdal.BuildVRT(str(vrt_path), vsi_paths, options=vrt_options)
        if vrt_ds is None:
            err = gdal.GetLastErrorMsg()
            raise RuntimeError(
                f'BuildVRT returned None for {variable} {year}. '
                f'{len(vsi_paths)} tiles ({test_info}), '
                f'first: {vsi_paths[0]}, GDAL error: {err}'
            )
        vrt_ds.FlushCache()
        vrt_ds = None

        # --- Create COG with NaN -> -9999 ---
        tmp_tif = tmpdir / f'{variable}_{year}.tmp.tif'
        cog_path = tmpdir / f'{variable}_{year}_cog.tif'

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
            endpoint_url=r2_endpoint,
            aws_access_key_id=os.environ['R2_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['R2_SECRET_ACCESS_KEY'],
            region_name='auto',
        )
        s3_key = f'{r2_prefix}/{variable}_{year}_cog.tif'
        r2_client.upload_file(str(cog_path), r2_bucket, s3_key)

    elapsed = time.time() - t0
    return variable, year, size_mb, elapsed


# ---------- Main ----------

def main():
    tasks = [(var, year) for var in VARIABLES for year in YEARS]
    print(f'Processing {len(VARIABLES)} variable(s) x {len(list(YEARS))} years = {len(tasks)} tasks')
    print(f'Variables: {", ".join(VARIABLES)}')
    print(f'Years: {YEARS[0]}–{YEARS[-1]}')
    print(f'Output: R2 bucket "{R2_BUCKET}"\n')

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

    # Map all (variable, year) pairs across workers
    futures = client.map(process_year, tasks)

    completed = 0
    for future in as_completed(futures):
        variable, year, size_mb, elapsed = future.result()
        completed += 1
        print(f'  [{completed}/{len(tasks)}] {variable} {year}: {size_mb:.0f} MB ({elapsed:.0f}s)')

    print('\nAll COGs uploaded.\n')

    client.close()
    cluster.close()
    print('Done!')


if __name__ == '__main__':
    main()
