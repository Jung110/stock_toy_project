import requests
import pandas as pd
import time
from sqlalchemy import create_engine,text
from datetime import datetime, timedelta
from bs4 import BeautifulSoup as bs

def getStockCode():
    """
    현재 있는 모든 종목의 종목명 , isin Code , 종목 코드를 크롤링 하여 가지고 Json형식으로 반환하는 함수 
    request와 BeautifulSoup4를 사용하였다.
    """

    # requests를 이용하여 해당 페이지를 가지고 온다.
    page = requests.get("https://www.ktb.co.kr/trading/popup/itemPop.jspx")
    # BeautifulSoup4를 이용하여 해당 페이지의 html 내용을 전부 가지고 온다.
    soup = bs(page.text ,"html.parser")
    # 그 중 tbody.tbody_content  중 tr 태그 중 td 중 a 태그 안에 있는 데이터를 전부 가지도 온다.
    elements = soup.select("tbody.tbody_content tr td a")
    stockCodeJson = {}


    for e in elements:
        # 필요한 데이터만 추출 하여 dict 형식으로 저장한다.
        data = str(e).split(",")
        code , codeName, isincode = data[1][1:-1] , data[2][1:-1].strip(), data[-1][1:13]
        stockCodeJson[code] = (codeName,isincode)
    return stockCodeJson

def saveStockCode(stockCodeJson:dict ,db_connection):
    """
    json 변수와 sqlalchemy의 create_engine으로 생성한 객체를 인수로 받아서 Json데이터를 MySql에 저장하는 함수
    """
    # 데이터를 넣기 편하게 Pandas DataFrame 으로 변환
    stockCodeDf = pd.DataFrame(stockCodeJson).T.reset_index()
    # 컬럼명 설정
    stockCodeDf.columns = ['srtnCd','itmsNm','isinCd']
    # 과거 종목 정보 데이터 버리기
    with db_connection.connect() as conn:
        conn.execute(text('TRUNCATE TABLE stock_code;'))
        conn.commit()
    # 데이터 Mysql에 넣기
    stockCodeDf.to_sql(name='stock_code', con=db_connection, if_exists='append',index=False)  
    


def getStockInfo(db_connection):
    ## 날짜 가져오기
    last_day = getLastDay(db_connection)

    # request data to API
    headers = {'Content-Type': 'application/json', 'charset': 'UTF-8', 'Accept': '*/*'}
    # for school
    # key_path = "/Users/c05/Desktop/learn/web/PipeLine_Project/key.txt"
    # for home
    # key_path = "C:\\Users\\AW17R4\\.appkey\\open_stock_api_key.txt"
    # for ec2
    key_path = "./key.txt"
    url = "https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo"


    with open(key_path,'r',encoding="UTF-8") as f:
        key = f.readline()

    params = {'serviceKey' : key
            , 'numOfRows' : 10000
            , 'pageNo' : 1
            , 'resultType' : "json"
            , 'beginBasDt' : last_day
            }

    response = requests.get(url ,params=params)
    total_count = response.json()['response']['body']['totalCount']
    if total_count == 0 :
        print("Nothing to Update")
        return 0
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
                        print(f"Error: {temp_df}")
                        break
                    continue
            time.sleep(0.5)



def getDatabaseConnection():
    # connect to  MySql DataBase
    # db_connection_str = 'mysql+pymysql://root:@localhost/stockDB'
    # for home
    # db_connection_str = 'mysql+pymysql://root:1234@localhost/stock_db'
    # for ec2
    db_connection_str = 'mysql+pymysql://stock_user:1234asde@172.31.13.248/stockDB'
    db_connection = create_engine(db_connection_str)
    conn = db_connection.connect()
    return db_connection ,conn


def getLastDay(db_connection):
    with db_connection.connect() as conn:
        last_day = pd.read_sql_query(text("SELECT basDt FROM stockDB.stockinfotable ORDER BY basDt DESC LIMIT 1;"), conn)
    start_day = (last_day + timedelta(days=1))['basDt'][0]
    result  = str(start_day.date()).replace("-","")
    return result



if __name__ == "__main__":
    db_connection,conn = getDatabaseConnection()
    getStockInfo(db_connection)
    stockCodeJson = getStockCode()
    saveStockCode(stockCodeJson,db_connection)
    conn.close()

    


    


    


