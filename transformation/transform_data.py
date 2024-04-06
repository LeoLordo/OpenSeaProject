import pandas as pd
from helpers.columns import table_schema


class Transform:
    def __init__(self, data_list: list):
        self.data_list = data_list

    def transform_data(self) -> pd.DataFrame:
        dataframe_list = []
        for data in self.data_list:
            collections = data["collections"]
            df = pd.DataFrame(collections)
            dataframe_list.append(df)
        concatenated_df = pd.concat(dataframe_list, ignore_index=True)
        filtered_df = concatenated_df[table_schema]
        return filtered_df



