import requests
import pandas as pd
import time
from sqlalchemy import create_engine,text
from datetime import datetime, timedelta
from bs4 import BeautifulSoup as bs

def getStockCode():
    page = requests.get("https://www.ktb.co.kr/trading/popup/itemPop.jspx")
    soup = bs(page.text ,"html.parser")


    elements = soup.select("tbody.tbody_content tr td a")
    stockCodeJson = {}


    for e in elements:
        data = str(e).split(",")
        code , codeName, isincode = data[1][1:-1] , data[2][1:-1].strip(), data[-1][1:13]
        stockCodeJson[code] = (codeName,isincode)
    return stockCodeJson

def saveStockCode(stockCodeJson:dict ,db_connection):
    stockCodeDf = pd.DataFrame(stockCodeJson).T.reset_index()
    stockCodeDf.columns = ['srtnCd','itmsNm','isinCd']
    with db_connection.connect() as conn:
        conn.execute(text('TRUNCATE TABLE stock_code;'))
        conn.commit()
    stockCodeDf.to_sql(name='stock_code', con=db_connection, if_exists='append',index=False)  
    


def getStockInfo(db_connection):
    ## 날짜 가져오기
    yesterday = datetime.today() - timedelta(days=1)
    beginBasDt=f'{yesterday.year}{yesterday.month }{yesterday.day}'

    # request data to API
    headers = {'Content-Type': 'application/json', 'charset': 'UTF-8', 'Accept': '*/*'}
    # for school
    # key_path = "/Users/c05/Desktop/learn/web/PipeLine_Project/key.txt"
    # for home
    # key_path = "C:\\Users\\AW17R4\\.appkey\\open_stock_api_key.txt"
    # for ec2
    key_path = "/home/ubuntu/apikey/key.txt"
    url = "https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo"


    with open(key_path,'r',encoding="UTF-8") as f:
        key = f.readline()

    params = {'serviceKey' : key
            , 'numOfRows' : 10000
            , 'pageNo' : 1
            , 'resultType' : "json"
            , 'beginBasDt' : beginBasDt
            }

    response = requests.get(url ,params=params)
    total_count = response.json()['response']['body']['totalCount']

    df = pd.DataFrame(response.json()['response']['body']['items']['item'])
    df.to_sql(name='stockinfotable', con=db_connection, if_exists='append',index=False)  
    if total_count > params['numOfRows']:
        for i in range(2,total_count//10000+1):
            params['pageNo'] = i
            errorCount = 1
            while True:
                try:
                    response = requests.get(url ,params=params,verify=True)
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



def getDatabaseConnection():
    # connect to  MySql DataBase
    # db_connection_str = 'mysql+pymysql://root:@localhost/stockDB'
    # for home
    # db_connection_str = 'mysql+pymysql://root:1234@localhost/stock_db'
    # for ec2
    db_connection_str = 'mysql+pymysql://root:1234@localhost/stock_db'
    db_connection = create_engine(db_connection_str)
    conn = db_connection.connect()
    return db_connection ,conn



if __name__ == "__main__":
    db_connection,conn = getDatabaseConnection()
    getStockInfo(db_connection)
    stockCodeJson = getStockCode()
    saveStockCode(stockCodeJson,db_connection)
    conn.close()

    


    


    


