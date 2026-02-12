"""
Generate a static WMTS Capabilities XML with all ESF COGs as named layers.

Each layer points to the titiler Lambda for tile serving.
The XML is hosted on GitHub Pages alongside the STAC catalog.

Usage:
    python build_wmts.py
"""

from pathlib import Path
from urllib.parse import quote

VARIABLES = {
    'agb': {'title': 'Aboveground Biomass', 'rescale': '0,250'},
    'agc': {'title': 'Aboveground Carbon', 'rescale': '0,125'},
    'bgc': {'title': 'Belowground Carbon', 'rescale': '0,75'},
}
YEARS = range(1990, 2024)
TITILER_BASE = 'https://vaiqqtybhb.execute-api.us-east-1.amazonaws.com'
R2_BUCKET = 'osc'
REPO_ROOT = Path(__file__).resolve().parent

# Bounding box (WGS84) for all layers — same grid
LOWER_CORNER = '-80.10777503499376 39.810569046663915'
UPPER_CORNER = '-70.0103053647419 45.92766968795332'

# WebMercatorQuad tile matrix limits (from titiler sample)
TILE_MATRIX_LIMITS = [
    (5, 11, 12, 8, 9),
    (6, 22, 24, 17, 19),
    (7, 45, 48, 35, 39),
    (8, 91, 97, 71, 78),
    (9, 182, 194, 142, 156),
    (10, 364, 388, 284, 312),
    (11, 729, 776, 568, 625),
    (12, 1458, 1553, 1136, 1251),
]


def build_layer(var, year, var_info):
    """Build a single WMTS layer XML block."""
    cog_s3 = f's3://{R2_BUCKET}/esf-{var}/{var}_{year}_cog.tif'
    encoded_s3 = quote(cog_s3, safe='')
    rescale = quote(var_info['rescale'], safe='')
    identifier = f'{var.upper()}_{year}'
    title = f'{var_info["title"]} {year}'

    template = (
        f'{TITILER_BASE}/cog/tiles/WebMercatorQuad'
        f'/{{TileMatrix}}/{{TileCol}}/{{TileRow}}.png'
        f'?url={encoded_s3}&amp;rescale={rescale}&amp;colormap_name=viridis'
    )

    limits_xml = ''
    for z, min_row, max_row, min_col, max_col in TILE_MATRIX_LIMITS:
        limits_xml += f"""
                    <TileMatrixLimits>
                    <TileMatrix>{z}</TileMatrix>
                    <MinTileRow>{min_row}</MinTileRow>
                    <MaxTileRow>{max_row}</MaxTileRow>
                    <MinTileCol>{min_col}</MinTileCol>
                    <MaxTileCol>{max_col}</MaxTileCol>
                    </TileMatrixLimits>"""

    return f"""        <Layer>
            <ows:Title>{title}</ows:Title>
            <ows:Identifier>{identifier}</ows:Identifier>
            <ows:WGS84BoundingBox crs="urn:ogc:def:crs:OGC:2:84">
                <ows:LowerCorner>{LOWER_CORNER}</ows:LowerCorner>
                <ows:UpperCorner>{UPPER_CORNER}</ows:UpperCorner>
            </ows:WGS84BoundingBox>
            <Style isDefault="true">
                <ows:Title>default</ows:Title>
                <ows:Identifier>default</ows:Identifier>
            </Style>
            <Format>image/png</Format>
            <TileMatrixSetLink>
                <TileMatrixSet>WebMercatorQuad</TileMatrixSet>
                <TileMatrixSetLimits>{limits_xml}
                </TileMatrixSetLimits>
            </TileMatrixSetLink>
            <ResourceURL format="image/png" resourceType="tile" template="{template}" />
        </Layer>"""


def build_wmts():
    """Generate the combined WMTS Capabilities XML."""
    getcaps_url = f'https://opensciencecomputing.github.io/esf-openfrontier/WMTSCapabilities.xml'

    layers = []
    for var, var_info in VARIABLES.items():
        for year in YEARS:
            layers.append(build_layer(var, year, var_info))

    layers_xml = '\n'.join(layers)

    # WebMercatorQuad TileMatrixSet definition (EPSG:3857, standard Google/OSM grid)
    tile_matrices = ''
    for z in range(0, 25):
        scale = 559082264.0287178 / (2 ** z)
        dim = 2 ** z
        tile_matrices += f"""
            <TileMatrix>
                <ows:Identifier>{z}</ows:Identifier>
                <ScaleDenominator>{scale}</ScaleDenominator>
                <TopLeftCorner>-20037508.3427892 20037508.3427892</TopLeftCorner>
                <TileWidth>256</TileWidth>
                <TileHeight>256</TileHeight>
                <MatrixWidth>{dim}</MatrixWidth>
                <MatrixHeight>{dim}</MatrixHeight>
            </TileMatrix>"""

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Capabilities xmlns="http://www.opengis.net/wmts/1.0"
    xmlns:ows="http://www.opengis.net/ows/1.1"
    xmlns:xlink="http://www.w3.org/1999/xlink"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xmlns:gml="http://www.opengis.net/gml"
    xsi:schemaLocation="http://www.opengis.net/wmts/1.0 http://schemas.opengis.net/wmts/1.0/wmtsGetCapabilities_response.xsd"
    version="1.0.0">
    <ows:ServiceIdentification>
        <ows:Title>ESF OpenFrontier - Landsat Ensemble Carbon Data</ows:Title>
        <ows:Abstract>Annual aboveground biomass (AGB), aboveground carbon (AGC), and belowground carbon (BGC) estimates for New York State, 1990-2023. 30m resolution, derived from Landsat ensemble models.</ows:Abstract>
        <ows:ServiceType>OGC WMTS</ows:ServiceType>
        <ows:ServiceTypeVersion>1.0.0</ows:ServiceTypeVersion>
    </ows:ServiceIdentification>
    <ows:ServiceProvider>
        <ows:ProviderName>ESF / Frontier Geospatial</ows:ProviderName>
        <ows:ServiceContact/>
    </ows:ServiceProvider>
    <ows:OperationsMetadata>
        <ows:Operation name="GetCapabilities">
            <ows:DCP>
                <ows:HTTP>
                    <ows:Get xlink:href="{getcaps_url}">
                        <ows:Constraint name="GetEncoding">
                            <ows:AllowedValues>
                                <ows:Value>RESTful</ows:Value>
                            </ows:AllowedValues>
                        </ows:Constraint>
                    </ows:Get>
                </ows:HTTP>
            </ows:DCP>
        </ows:Operation>
        <ows:Operation name="GetTile">
            <ows:DCP>
                <ows:HTTP>
                    <ows:Get xlink:href="{TITILER_BASE}/cog/tiles/">
                        <ows:Constraint name="GetEncoding">
                            <ows:AllowedValues>
                                <ows:Value>RESTful</ows:Value>
                            </ows:AllowedValues>
                        </ows:Constraint>
                    </ows:Get>
                </ows:HTTP>
            </ows:DCP>
        </ows:Operation>
    </ows:OperationsMetadata>
    <Contents>
{layers_xml}
        <TileMatrixSet>
            <ows:Identifier>WebMercatorQuad</ows:Identifier>
            <ows:SupportedCRS>urn:ogc:def:crs:EPSG::3857</ows:SupportedCRS>{tile_matrices}
        </TileMatrixSet>
    </Contents>
</Capabilities>
"""

    out_path = REPO_ROOT / 'WMTSCapabilities.xml'
    out_path.write_text(xml)
    print(f'Wrote {out_path} ({len(VARIABLES)} variables x {len(list(YEARS))} years = {len(layers)} layers)')


if __name__ == '__main__':
    build_wmts()
