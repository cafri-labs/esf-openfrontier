"""
Build COGs for ESF Landsat Ensemble Data.

Converts ESF Landsat ensemble data from per-year S3 tiles into
COGs uploaded to Cloudflare R2.

Each (variable, year) pair is processed in parallel using ProcessPoolExecutor.
Recommended: run on an EC2 r5.4xlarge (16 vCPU, 128 GB) or larger.

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
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import fsspec
import numpy as np
import rasterio
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

R2_ENDPOINT = 'https://9b09ae0d6de9d5f05feda24c975cf645.r2.cloudflarestorage.com'
R2_BUCKET = 'osc'

# Number of parallel workers.
# Each task needs ~6-8 GB RAM. Set based on available memory:
#   r5.4xlarge  (128 GB) -> 16 workers
#   r5.2xlarge  (64 GB)  ->  8 workers
#   r5.xlarge   (32 GB)  ->  4 workers
N_WORKERS = 16

# Credentials env var names (must be set in environment before running)
CRED_ENV_VARS = [
    'ESF_AWS_ACCESS_KEY_ID',
    'ESF_AWS_SECRET_ACCESS_KEY',
    'R2_ACCESS_KEY_ID',
    'R2_SECRET_ACCESS_KEY',
]


# ---------- Worker function ----------

def process_year(args):
    """Process a single (variable, year): VRT -> COG -> upload to R2."""
    variable, year = args

    import os
    import tempfile
    import time
    from pathlib import Path

    import boto3
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
    r2_endpoint = 'https://9b09ae0d6de9d5f05feda24c975cf645.r2.cloudflarestorage.com'
    r2_bucket = 'osc'
    r2_prefix = f'esf-{variable}'

    # Configure GDAL for S3 reads
    esf_key = os.environ['ESF_AWS_ACCESS_KEY_ID']
    esf_secret = os.environ['ESF_AWS_SECRET_ACCESS_KEY']

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
    print(f'Output: R2 bucket "{R2_BUCKET}"')
    print(f'Workers: {N_WORKERS}\n')

    # Verify credentials are set before spawning workers
    for var in CRED_ENV_VARS:
        if not os.environ.get(var):
            raise RuntimeError(f'Missing environment variable: {var}')

    # Workers inherit env vars from the parent process
    completed = 0
    errors = []

    with ProcessPoolExecutor(max_workers=N_WORKERS) as executor:
        futures = {executor.submit(process_year, task): task for task in tasks}
        for future in as_completed(futures):
            task = futures[future]
            try:
                variable, year, size_mb, elapsed = future.result()
                completed += 1
                print(f'  [{completed}/{len(tasks)}] {variable} {year}: {size_mb:.0f} MB ({elapsed:.0f}s)')
            except Exception as e:
                errors.append((task, str(e)))
                print(f'  ERROR {task}: {e}')

    print(f'\nDone: {completed} succeeded, {len(errors)} failed.')
    if errors:
        for task, msg in errors:
            print(f'  FAILED {task}: {msg}')


if __name__ == '__main__':
    main()
