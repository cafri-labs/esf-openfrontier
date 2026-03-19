"""
Upload per-county FlatGeobuf files from parcels/ to Cloudflare R2.

Usage:
    python upload_parcels.py
"""

import os
from pathlib import Path

import boto3
from botocore.config import Config

R2_ENDPOINT   = 'https://9b09ae0d6de9d5f05feda24c975cf645.r2.cloudflarestorage.com'
R2_ACCESS_KEY = 'd4ab48979081787f8b7776f95db02d54'
R2_SECRET_KEY = 'e777964b506eae725411c9946c0595df8cce9a42773cbce9e6a8896e1c5e8227'
R2_BUCKET     = 'cafri-public'
R2_PREFIX     = 'parcels'

PARCELS_DIR = Path('parcels')

s3 = boto3.client(
    's3',
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    config=Config(
        request_checksum_calculation='when_required',
        response_checksum_validation='when_required',
    ),
)

files = sorted(PARCELS_DIR.glob('*.fgb'))
print(f'Uploading {len(files)} file(s) to r2://{R2_BUCKET}/{R2_PREFIX}/')

for fgb in files:
    key = f'{R2_PREFIX}/{fgb.name}'
    size_mb = fgb.stat().st_size / 1e6
    print(f'  {fgb.name} ({size_mb:.1f} MB) ... ', end='', flush=True)
    s3.upload_file(
        str(fgb),
        R2_BUCKET,
        key,
        ExtraArgs={'ContentType': 'application/octet-stream'},
    )
    print('done')

print('Upload complete.')
