import pandas as pd
import json


class LoadData:
    def __init__(self, raw_data: list, raw_data_path: str, filtered_data: pd.DataFrame, filtered_data_path: str):
        self.raw_data = raw_data
        self.raw_data_path = raw_data_path
        self.filtered_data = filtered_data
        self.filtered_data_path = filtered_data_path

    def insert_raw_data(self):
        with open(self.raw_data_path, "w") as file:
            for data in self.raw_data:
                json.dump(data, file, indent=4)

    def insert_transformed_data(self):
        with open(self.filtered_data_path, "w") as file:
            for data in self.filtered_data:
                json.dump(data, file, indent=4)

