from sqlalchemy import create_engine,text
import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from datetime import datetime
from sklearn.metrics import mean_absolute_error
import warnings
warnings.filterwarnings('ignore')




def getDataFromDB():
    db_connection_str = 'mysql+pymysql://root:1234@localhost/stock_db'
    db_connection = create_engine(db_connection_str)
    with db_connection.connect() as conn:
        data_df = pd.read_sql_table("stockinfotable", conn)
    return data_df , db_connection

def getCodeList(data_df):
    return list(data_df['srtnCd'].unique())

def predictFutureStockPrice(srtnCd:str ,conn ,data_df):

    
    today = datetime.today().strftime("%Y%m%d")
    x_cols = ['clpr','vs','fltRt','mkp','lopr','trqu','lstgStCnt']
    y_col = 'tmkp'

    all_data_df = data_df.loc[data_df['srtnCd']==srtnCd].loc[:,['clpr','vs','fltRt','mkp','lopr','trqu','lstgStCnt']]
    data_count = len(all_data_df)

    if data_count < 100:
        return 0

    all_data_df['tmkp'] = 0 
    all_data_df.iloc[:data_count-1,-1] = all_data_df.iloc[1:,3]

    today_data_x = pd.DataFrame(all_data_df.iloc[-1,:-1])
    all_data_df = all_data_df[:data_count-1]

    ss = StandardScaler()

    x_df = ss.fit_transform(all_data_df.loc[:,x_cols])
    today_data_x = ss.fit_transform(today_data_x)
    y_df = all_data_df.loc[:,y_col]

    x_train , x_test  = train_test_split(x_df,test_size=0.3,shuffle=False)
    y_train , y_test = train_test_split(y_df,test_size=0.3,shuffle=False)

    model = XGBRegressor()

    model.fit(x_train , y_train)
    y_pred = model.predict(x_test)

    mae = mean_absolute_error(y_test ,y_pred)

    result =model.predict(today_data_x.reshape(1,-1))

    result = np.round(result)

    pred_df = pd.DataFrame([today,srtnCd,result[0],mae]).T
    pred_df.columns = ['basDt','srtnCd','mkp','mae']
    

    db_connection_str = 'mysql+pymysql://root:1234@localhost/stock_db'
    db_connection = create_engine(db_connection_str, pool_pre_ping=True)

    try:
        pred_df.to_sql(name='stockpredicttable' , con=conn , if_exists='append',index=False)
    except:
        print(srtnCd)
    return 1


if __name__ == "__main__":
    data_df,db_connection = getDataFromDB()

    code_list = getCodeList(data_df)

    for code in code_list:
        predictFutureStockPrice(code,db_connection,data_df)
        

