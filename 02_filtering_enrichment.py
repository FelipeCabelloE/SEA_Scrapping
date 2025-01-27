import geopandas as gpd
from rich import print


def main():
    # Download Chile's admin level 3 (communes) data from GADM
    url = "https://geodata.ucdavis.edu/gadm/gadm4.1/json/gadm41_CHL_3.json"
    gdf = gpd.read_file(url)

    print(gdf["NAME_3"])


if __name__ == "__main__":
    main()
