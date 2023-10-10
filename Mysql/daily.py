import requests
import pandas as pd
import time
from sqlalchemy import create_engine
from datetime import datetime


if __name__ == "__main__":
    # connect to  MySql DataBase
    # db_connection_str = 'mysql+pymysql://root:@localhost/stockDB'
    # for home
    db_connection_str = 'mysql+pymysql://root:1234@localhost/stock_db'
    db_connection = create_engine(db_connection_str)
    conn = db_connection.connect()

    ## 날짜 가져오기
    beginBasDt=f'{datetime.today().year}{datetime.today().month }{datetime.today().day}'

    # request data to API
    headers = {'Content-Type': 'application/json', 'charset': 'UTF-8', 'Accept': '*/*'}
    # for school
    # key_path = "/Users/c05/Desktop/learn/web/PipeLine_Project/key.txt"
    # for home
    key_path = "C:\\Users\\AW17R4\\.appkey\\open_stock_api_key.txt"
    url = "https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo"


    with open(key_path,'r',encoding="UTF-8") as f:
        key = f.readline()

    params = {'serviceKey' : key
            , 'numOfRows' : 10000
            , 'pageNo' : 1
            , 'resultType' : "json"
            , 'beginBasDt' : '20210101'
            }

    response = requests.get(url ,params=params)
    total_count = response.json()['response']['body']['totalCount']


    df = pd.DataFrame(response.json()['response']['body']['items']['item'])
    df.to_sql(name='stockinfotable', con=db_connection, if_exists='append',index=False)  
    if total_count > params['numOfRows']:
        for i in range(2,total_count//10000+1):
            params['pageNo'] = i

            while True:
                errorCount = 1
                try:
                    response = requests.get(url ,params=params)
                    temp_df =  pd.DataFrame(response.json()['response']['body']['items']['item'])
                    temp_df.to_sql(name='stockinfotable', con=db_connection, if_exists='append',index=False)
                    break
                except Exception:
                    time.sleep(5)
                    print(f'error:{errorCount}')
                    if errorCount==10:
                        break
                    continue
            time.sleep(0.5)
