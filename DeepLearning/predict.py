from sqlalchemy import create_engine
import pandas as pd
import numpy as np

# 데이터 읽어오기
db_connection_str = 'mysql+pymysql://root:1234@localhost/stock_db'
db_connection = create_engine(db_connection_str)
conn = db_connection.connect()

df = pd.read_sql_table("stockinfotable", conn)

