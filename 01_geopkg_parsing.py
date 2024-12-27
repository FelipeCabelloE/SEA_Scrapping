from urllib.parse import parse_qs, urlparse

import geopandas as gpd
import pandas as pd
from bs4 import BeautifulSoup
from rich import print


# Function to extract data from HTML
def extract_info(html):
    soup = BeautifulSoup(html, "html.parser")
    # Extract text inside <b> tag
    bold_text = soup.find("b").get_text(strip=True) if soup.find("b") else None
    # Extract src attribute of <iframe>
    iframe_src = soup.find("iframe")["src"] if soup.find("iframe") else None
    # Extract the id parameter from the iframe_src
    id_value = None
    if iframe_src:
        parsed_url = urlparse(iframe_src)
        id_value = parse_qs(parsed_url.query).get("id", [None])[0]
    # Construct the new URL
    constructed_link = (
        f"https://seia.sea.gob.cl/expediente/ficha/fichaPrincipal.php?modo=normal&id_expediente={id_value}"
        if id_value
        else None
    )
    return bold_text, iframe_src, constructed_link


def main():
    gdf_proyectos = gpd.read_file("./DATA/proyectos.gpkg")
    print(gdf_proyectos.info())
    gdf_proyectos = gdf_proyectos.loc[:, ["Name", "description", "geometry"]]
    print(gdf_proyectos.info())
    print(gdf_proyectos["description"])
    gdf_proyectos[["bold_text", "iframe_src", "constructed_link"]] = gdf_proyectos[
        "description"
    ].apply(lambda x: pd.Series(extract_info(x)))
    print(gdf_proyectos.info())
    print(gdf_proyectos["constructed_link"][0])


if __name__ == "__main__":
    main()
