"""
Build a STAC catalog for the ESF AGB COGs on Cloudflare R2.

Saves catalog.json + esf-agb/ to the repo root for GitHub Pages hosting.

Usage:
    python build_stac.py
"""

from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import pystac
import rasterio
from pyproj import Transformer
from shapely.geometry import box, mapping

# ---------- Configuration ----------

YEARS = range(1990, 2024)

R2_PREFIX = 'esf-agb'
PUBLIC_BASE_URL = f'https://pub-59649a08584b41c490cb84732702591a.r2.dev/{R2_PREFIX}'

REPO_ROOT = Path(__file__).resolve().parent


def build_stac():
    """Build a STAC catalog from the uploaded COGs and save to repo root."""
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
            stac_extensions=[
                'https://stac-extensions.github.io/web-map-links/v1.2.0/schema.json',
            ],
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
        encoded_url = quote(cog_url, safe='')
        xyz_href = (
            f'https://titiler.xyz/cog/tiles/WebMercatorQuad/{{z}}/{{x}}/{{y}}'
            f'?url={encoded_url}'
            f'&rescale=0,250&colormap_name=viridis'
        )
        item.add_link(pystac.Link(
            rel='xyz',
            target=xyz_href,
            media_type='image/png',
            title=f'AGB {year} XYZ tiles',
        ))
        collection.add_item(item)

    catalog = pystac.Catalog(
        id='esf-openfrontier',
        title='ESF OpenFrontier',
        description='Geospatial datasets from the ESF / Frontier Geospatial collaboration.',
    )
    catalog.add_child(collection)
    catalog.normalize_hrefs(str(REPO_ROOT))
    catalog.validate_all()

    catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED,
                 dest_href=str(REPO_ROOT))

    print(f'Saved catalog.json + esf-agb/ to {REPO_ROOT}')


if __name__ == '__main__':
    print('Building STAC catalog...')
    build_stac()
    print('Done!')
