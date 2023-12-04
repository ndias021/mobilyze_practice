import io
import os
import zipfile
import requests
import logging

from src.core.base import Base


class RawJob(Base):
    layer = "RAW"
    source = None
    target = "./data/RAW/"

    def _get_data(self, url):
        try:
            response = requests.get(url)
            response.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
                zip_ref.extractall(self.target)
            extraction_status = "Extraction successful."
        except requests.exceptions.HTTPError as err:
            extraction_status = f"HTTP Error: {err}"
        except Exception as e:
            extraction_status = f"An error occurred: {e}"
        logging.info(extraction_status)

    def process(self):
        if not os.path.exists(self.target):
            os.makedirs(self.target)
        data_2006 = "https://ec.europa.eu/eurostat/cache/GISCO/geodatafiles/GEOSTAT_Grid_POP_2006_1K.zip"
        data_2021 = "https://gisco-services.ec.europa.eu/census/2021/Eurostat_Census-GRID_2021_V1-0.zip"
        self._get_data(data_2006)
        logging.info(f"Downloaded 2006 data to {self.target}")
        self._get_data(data_2021)
        logging.info(f"Downloaded 2021 data to {self.target}")

