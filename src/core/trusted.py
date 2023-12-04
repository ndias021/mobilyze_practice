import os
import logging
import pandas as pd
import geopandas as gpd


from src.core.base import Base


class TrustedJob(Base):
    layer = "TRUSTED"
    source = "./data/RAW/"
    target = "./data/TRUSTED/"

    @staticmethod
    def _convert_grd_id_to_cellcode( grd_id):
        northing = grd_id.split('N')[1][0:4]
        easting = grd_id.split('N')[1][8:12]
        cellcode = f"1kmN{northing}E{easting}"
        return cellcode

    def _cleanse_and_filter(self, data_2006, pop_2006_data, data_2021):
        # Year: 2006
        data_2006_sk = data_2006.merge(pop_2006_data, left_on='GRD_INSPIR', right_on='GRD_ID', how='inner')
        data_2006_sk = data_2006_sk[data_2006_sk["CNTR_CODE"].str.contains("SK")]
        data_2006_sk = data_2006_sk[data_2006_sk["YEAR"] == 2006]
        data_2006_sk = data_2006_sk.reset_index(drop=True)
        data_2006_sk.rename(columns={
            "GRD_ID": "grd_id",
            "POP_TOT": "population",
        }, inplace=True)
        data_2006_sk = data_2006_sk[["grd_id", "population", "geometry"]]
        tmp1 = data_2006_sk.to_crs(4326)
        tmp1['lon'] = tmp1.centroid.x
        tmp1['lat'] = tmp1.centroid.y
        data_2006_sk = data_2006_sk.merge(tmp1, on='grd_id', how='inner', suffixes=("", "_y"))
        data_2006_sk = data_2006_sk[["grd_id", "population", "lon", "lat", "geometry"]]
        data_2006_sk = data_2006_sk.to_crs(3035)

        # Year: 2021
        data_2021.rename(columns={
            "GRD_ID": "grd_id",
            "OBS_VALUE_T": "population"
        }, inplace=True)
        data_2021['converted_grd_id'] = data_2021['grd_id'].apply(self._convert_grd_id_to_cellcode)
        data_2021.drop(columns=["grd_id"], inplace=True)
        data_2021.rename(columns={"converted_grd_id": "grd_id"})
        data_2021_sk = data_2021.merge(data_2006_sk, left_on='converted_grd_id', right_on='grd_id', how='inner',
                                       suffixes=("", "_y"))
        data_2021_sk = data_2021_sk[["grd_id", "population", "lon", "lat", "geometry"]]

        data_2021_sk = data_2021_sk.to_crs(3035)

        return data_2006_sk, data_2021_sk

    def process(self):
        logging.info("Reading 2006 file ...")
        data_2006 = gpd.read_file(f"{self.source}Grid_ETRS89_LAEA_1K_ref_GEOSTAT_2006.shp")

        logging.info("Reading 2006 - population file ...")
        pop_2006_data = pd.read_csv(f"{self.source}GEOSTAT_grid_EU_POP_2006_1K_V1_1_1.csv",
                                    delimiter=";")

        logging.info("Reading 2021 file ...")
        data_2021 = gpd.read_file(f"{self.source}ESTAT_Census_2021_V1-0.gpkg")

        logging.info("Cleansing and preparing 2006 and 2021 files ...")
        data_2006_sk, data_2021_sk = self._cleanse_and_filter(data_2006, pop_2006_data, data_2021)

        logging.info(f"Writing output to {self.target} ...")
        if not os.path.exists(self.target):
            os.makedirs(self.target)
        data_2006_sk.to_file(f"{self.target}Slovakia_2006.shp")
        data_2021_sk.to_file(f"{self.target}Slovakia_2021.shp")

