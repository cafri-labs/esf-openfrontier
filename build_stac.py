"""
Build a STAC catalog for the ESF Landsat Ensemble COGs on Cloudflare R2.

Saves catalog.json + esf-{var}/ directories to the repo root for GitHub Pages.

Usage:
    python build_stac.py
"""

from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import pystac
import requests
from pyproj import Transformer
from shapely.geometry import box, mapping

# ---------- Configuration ----------

VARIABLES = {
    'agb': {
        'title': 'ESF Landsat Ensemble Aboveground Biomass',
        'description': (
            'Annual aboveground biomass (AGB) estimates (Mg/ha) for '
            'New York State, derived from Landsat ensemble models. '
            '30 m resolution, EPSG:5070. Version 2.0.0.'
        ),
        'rescale': '0%2C250',
    },
    'agc': {
        'title': 'ESF Landsat Ensemble Aboveground Carbon',
        'description': (
            'Annual aboveground carbon (AGC) estimates (Mg C/ha) for '
            'New York State, derived from Landsat ensemble models. '
            '30 m resolution, EPSG:5070. Version 2.0.0.'
        ),
        'rescale': '0%2C125',
    },
    'bgc': {
        'title': 'ESF Landsat Ensemble Belowground Carbon',
        'description': (
            'Annual belowground carbon (BGC) estimates (Mg C/ha) for '
            'New York State, derived from Landsat ensemble models. '
            '30 m resolution, EPSG:5070. Version 2.0.0.'
        ),
        'rescale': '0%2C75',
    },
}

YEARS = range(1990, 2024)
TITILER_BASE = 'https://rq1vo2j0e9.execute-api.us-east-1.amazonaws.com'
R2_BUCKET = 'cafri'

REPO_ROOT = Path(__file__).resolve().parent


def build_stac():
    """Build a STAC catalog from the uploaded COGs and save to repo root."""
    # Get spatial metadata from a sample COG via titiler (bucket is private)
    sample_s3 = f's3://{R2_BUCKET}/esf-agb/agb_{YEARS[0]}_cog.tif'
    info = requests.get(f'{TITILER_BASE}/cog/info', params={'url': sample_s3}).json()
    bounds = info['bounds']  # in native CRS (EPSG:5070)
    native_shape = [info['height'], info['width']]
    # Build transform from bounds and shape: (xres, 0, xmin, 0, -yres, ymax)
    xres = (bounds[2] - bounds[0]) / info['width']
    yres = (bounds[3] - bounds[1]) / info['height']
    native_transform = [xres, 0.0, bounds[0], 0.0, -yres, bounds[3]]

    transformer = Transformer.from_crs('EPSG:5070', 'EPSG:4326', always_xy=True)
    west, south = transformer.transform(bounds[0], bounds[1])
    east, north = transformer.transform(bounds[2], bounds[3])
    bbox_4326 = [west, south, east, north]
    geometry_4326 = mapping(box(*bbox_4326))
    proj_bbox = [bounds[0], bounds[1], bounds[2], bounds[3]]

    catalog = pystac.Catalog(
        id='esf-openfrontier',
        title='ESF OpenFrontier',
        description='Geospatial datasets from the ESF / Frontier Geospatial collaboration.',
    )

    for var, var_info in VARIABLES.items():
        collection = pystac.Collection(
            id=f'esf-{var}',
            title=var_info['title'],
            description=var_info['description'],
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

        rescale = var_info['rescale']

        for year in YEARS:
            cog_s3 = f's3://{R2_BUCKET}/esf-{var}/{var}_{year}_cog.tif'
            encoded_s3 = quote(cog_s3, safe='')
            item = pystac.Item(
                id=f'{var}-{year}',
                geometry=geometry_4326,
                bbox=bbox_4326,
                datetime=datetime(year, 1, 1, tzinfo=timezone.utc),
                properties={
                    'proj:epsg': 5070,
                    'proj:shape': native_shape,
                    'proj:transform': native_transform,
                    'proj:bbox': proj_bbox,
                },
                stac_extensions=[
                    'https://stac-extensions.github.io/web-map-links/v1.2.0/schema.json',
                ],
            )
            item.add_asset(
                'data',
                pystac.Asset(
                    href=cog_s3,
                    media_type=pystac.MediaType.COG,
                    roles=['data'],
                    title=f'{var.upper()} {year}',
                ),
            )
            titiler_params = f'url={encoded_s3}&rescale={rescale}&colormap_name=viridis'
            xyz_href = (
                f'{TITILER_BASE}/cog/tiles/WebMercatorQuad/{{z}}/{{x}}/{{y}}'
                f'?{titiler_params}'
            )
            item.add_link(pystac.Link(
                rel='xyz',
                target=xyz_href,
                media_type='image/png',
                title=f'{var.upper()} {year} XYZ tiles',
            ))
            wmts_href = (
                f'{TITILER_BASE}/cog/WMTSCapabilities.xml'
                f'?{titiler_params}'
            )
            wmts_link = pystac.Link(
                rel='wmts',
                target=wmts_href,
                media_type='application/xml',
                title=f'{var.upper()} {year} WMTS',
            )
            wmts_link.extra_fields['wmts:layer'] = f'{var}_{year}'
            item.add_link(wmts_link)
            thumbnail_href = (
                f'{TITILER_BASE}/cog/preview.png'
                f'?{titiler_params}&max_size=512'
            )
            item.add_asset(
                'thumbnail',
                pystac.Asset(
                    href=thumbnail_href,
                    media_type='image/png',
                    roles=['thumbnail'],
                    title=f'{var.upper()} {year} preview',
                ),
            )
            collection.add_item(item)

        catalog.add_child(collection)

    catalog.normalize_hrefs(str(REPO_ROOT))
    catalog.validate_all()

    catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED,
                 dest_href=str(REPO_ROOT))

    print(f'Saved catalog.json + esf-{{agb,agc,bgc}}/ to {REPO_ROOT}')


if __name__ == '__main__':
    print('Building STAC catalog...')
    build_stac()
    print('Done!')
