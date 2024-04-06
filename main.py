import os
import dotenv
from extraction.exctract_data import Extract
from transformation.transform_data import Transform
from load.load_data import LoadData
from load.orm import ORM

dotenv.load_dotenv()

TABLE_NAME = os.getenv("TABLE_NAME")
API_KEY = os.getenv("API_KEY")
URL = os.getenv("URL")
RAW_DATA = os.getenv("RAW_DATA")
FORMATTED_DATA_NAME = os.getenv("FORMATTED_DATA_NAME")
def main():
    raw_data = Extract(api_key=API_KEY, url=URL).extract_collections()
    transformed_data = Transform(data_list=raw_data).transform_data()
    LoadData(raw_data=raw_data, raw_data_path=RAW_DATA, filtered_data=transformed_data, filtered_data_path=FORMATTED_DATA_NAME)
    orm = ORM(host='localhost',
              dbname='postgres',
              user='postgres',
              password='123456789',
              port='5432',
              table_name=TABLE_NAME,
              data=transformed_data)
    # orm.create_table()
    # orm.insert_data()
    # print(orm.filter_data(like="mob", column_name="twitter_username"))
    # print(orm.read_data(limit="5"))
    # orm.remove_column(column_name="owner")
    # orm.delete_table(cur_close=True)

    return {"message": "SUCCESS"}


if __name__ == "__main__":
    main()
