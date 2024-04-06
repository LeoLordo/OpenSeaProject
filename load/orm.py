import psycopg2
from helpers.columns import table_schema_dict
from psycopg2.extras import Json


class ORM:
    def __init__(self, host: str, dbname: str, user: str, password: str, port: str, table_name: str, data):
        self.conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        self.cur = self.conn.cursor()
        self.table_name = table_name
        self.data = data

    def create_table(self, cur_close=False):
        columns_and_types = ',\n'.join([f"{column} {data_type}" for column, data_type in table_schema_dict.items()])
        create_table_qyery = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                    {columns_and_types}
            )
        """
        self.cur.execute(create_table_qyery)
        self.conn.commit()
        if cur_close:
            self.cur.close()
            self.conn.close()

    def delete_table(self,  cur_close=False):
        query = f"DROP TABLE {self.table_name};"
        self.cur.execute(query)
        self.conn.commit()
        if cur_close:
            self.cur.close()
            self.conn.close()

    def insert_data(self,  cur_close=False):
        for _, row in self.data.iterrows():
            row['contracts'] = Json(row['contracts'])
            insert_query = f"INSERT INTO {self.table_name} ({', '.join(self.data.columns)}) VALUES ({', '.join(['%s']*len(self.data.columns))})"
            self.cur.execute(insert_query,tuple(row))
            self.conn.commit()
        if cur_close:
            self.cur.close()
            self.conn.close()

    def read_data(self, limit: str = None, order_by: str = None, cur_close=False) -> list:
        select_query = f"""
            SELECT * FROM {self.table_name}
        """
        if limit:
            select_query += f"\nLIMIT {limit}"
        if order_by:
            select_query += f"\nORDER BY {order_by}"
        self.cur.execute(select_query)
        rows = self.cur.fetchall()

        if cur_close:
            self.cur.close()
            self.conn.close()
        return rows

    def remove_column(self, column_name, cur_close=False):
        if type(column_name) is str:
            remove_column_query = f"""
                ALTER TABLE {self.table_name}
                DROP COLUMN {column_name}
            """
            self.cur.execute(remove_column_query)
        elif type(column_name) is list:
            for column in column_name:
                remove_column_query = f"""
                    ALTER TABLE {self.table_name}
                    DROP COLUMN {column}
                """
                self.cur.execute(remove_column_query)
        self.conn.commit()
        if cur_close:
            self.cur.close()
            self.conn.close()

    def filter_data(self, like: str = None, ilike: str = None, column_name: str = None) -> list:
        if like:
            select_query = f"""
                SELECT * FROM {self.table_name}
                WHERE {column_name} LIKE '%{like}%'  
            """
            self.cur.execute(select_query)
        elif ilike:
            select_query = f"""
                SELECT * FROM {self.table_name}
                WHERE {column_name} ILIKE '%{ilike}%' 
            """
            self.cur.execute(select_query)
        return self.cur.fetchall()




# data_list = Extract(api_key="6afc226105fd474b85998fe4ea531ac9", url="https://api.opensea.io/api/v2/collections").extract_collections()
# df = Transform(data_list).transform_data()
# print(df)
# orm = ORM(host='localhost', dbname='postgres', user='postgres', password='123456789', port='5432', table_name="collections", data=df)
# orm.create_table()
# # orm.delete_table()
# orm.insert_data()

# print(orm.read_data())
# orm.remove_column(column_name=["name", "image_url"])
# print(orm.filter_data(ilike="%M%", column_name="twitter_username"))
# mobk2301
