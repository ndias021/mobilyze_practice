import os
import geopandas as gpd


from src.core.base import Base


class EnrichedJob(Base):
    layer = "ENRICHED"
    source = "./data/TRUSTED/"
    target = "./data/ENRICHED/"

    def process(self):
        data_2006_sk = gpd.read_file(f"{self.source}Slovakia_2006.shp")
        data_2021_sk = gpd.read_file(f"{self.source}Slovakia_2021.shp")

        merged = data_2006_sk.merge(data_2021_sk, on="grd_id", how="inner", suffixes=("_2006", "_2021"))
        merged['relative_changes'] = ((merged['population_2021'] - merged['population_2006']) /
                                      merged['population_2006'])
        merged["Average population per 1sq km in 2021"] = merged["population_2021"].mean()
        merged["Median population per 1sq km in 2021"] = merged["population_2021"].median()
        merged = merged[[
            "lon_2021",
            "lat_2021",
            "population_2006",
            "population_2021",
            "relative_changes",
            "Average population per 1sq km in 2021",
            "Median population per 1sq km in 2021"]]

        merged.rename(columns={
            "lon_2021": "Longitude",
            "lat_2021": "Latitude",
            "population_2006": "Population in 2006",
            "population_2021": "Population in 2021",
            "relative_changes": "Relative changes"
        }, inplace=True)

        final_results = merged.sort_values(by='Relative changes', ascending=False).head(5)

        if not os.path.exists(self.target):
            os.makedirs(self.target)
        merged.to_csv(f"{self.target}all_results.csv", index=False)
        final_results[[
            "Longitude",
            "Latitude",
            "Population in 2006",
            "Population in 2021",
            "Relative changes"]].to_csv(f"{self.target}attachment_1.csv", index=False)
        final_results[[
            "Average population per 1sq km in 2021",
            "Median population per 1sq km in 2021"]].to_csv(f"{self.target}attachment_2.csv", index=False)

