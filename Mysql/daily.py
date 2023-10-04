import requests
import pandas as pd
import time
from sqlalchemy import create_engine

if __name__ == "__main__":
    # connect to  MySql DataBase
    db_connection_str = 'mysql+pymysql://root:@localhost/stockDB'
    db_connection = create_engine(db_connection_str)
    conn = db_connection.connect()

    # request data to API
    headers = {'Content-Type': 'application/json', 'charset': 'UTF-8', 'Accept': '*/*'}
    key_path = "/Users/c05/Desktop/learn/web/PipeLine_Project/key.txt"
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
    df.to_sql(name='stockInfoTable', con=db_connection, if_exists='replace',index=False)  
    if total_count > params['numOfRows']:
        for i in range(2,total_count//10000+1):
            params['pageNo'] = i

            try: 
                response = requests.get(url ,params=params)
            except Exception:
                time.sleep(1)
                response = requests.get(url ,params=params)
            # print(i)
            try:
                temp_df =  pd.DataFrame(response.json()['response']['body']['items']['item'])
                temp_df.to_sql(name='stockInfoTable', con=db_connection, if_exists='append',index=False)
            except Exception:
                print(temp_df.head(1))
                print('error')
                continue