import bitfinex
import datetime
import time
import os
import pandas as pd
def date_to_unix(l_date):
    time = datetime.datetime(
        int(l_date[0]),
        int(l_date[0]),
        int(l_date[0]),
        int(l_date[0]),
        int(l_date[0])
    )
    return time.mktime(time.timetuple()) * 1000
def unix_to_date(unix):
    ts = int(unix)/1000
    # if you encounter a "year is out of range" error the timestamp
    # may be in milliseconds, try `ts /= 1000` in that case
    return [
    int(datetime.datetime.utcfromtimestamp(ts).strftime("%Y")),
    int(datetime.datetime.utcfromtimestamp(ts).strftime("%m")),
    int(datetime.datetime.utcfromtimestamp(ts).strftime("%d")),
    int(datetime.datetime.utcfromtimestamp(ts).strftime("%H")),
    int(datetime.datetime.utcfromtimestamp(ts).strftime("%M"))
    ]
def unix_date_string(unix):
    ts = int(unix)/1000
    # if you encounter a "year is out of range" error the timestamp
    # may be in milliseconds, try `ts /= 1000` in that case
    return str(datetime.datetime.utcfromtimestamp(ts).strftime("%Y"))+" , "+str(datetime.datetime.utcfromtimestamp(ts).strftime("%m"))+" , "+str(datetime.datetime.utcfromtimestamp(ts).strftime("%d"))+" , "+str(datetime.datetime.utcfromtimestamp(ts).strftime("%H"))+" , "+str(datetime.datetime.utcfromtimestamp(ts).strftime("%M"))
#unix_to_date(1522525080000+i*60000*10))
#In The Name Of God
df=pd.DataFrame({"UNIX" : [1493942400000],"OPEN" : [1550],"HIGH" :[1557.53],"LOW" : [1549.69],"CLOSE" : [1550],"VOLUME":[164.7452541]},index=[unix_date_string("1493942400000")])
def updater(df,unix_2):
    unix=df["UNIX"]
    unix_1=unix[len(unix)-1]
    api_v2 = bitfinex.bitfinex_v2.api_v2()
    # Define query parameters
    pair = 'btcusd' # Currency pair of interest
    bin_size = '1m' # This will return minute data
    limit = 1000    # We want the maximum of 1000 data points 
    # Define the start date
    l_unix=[]
    while unix_1<unix_2:
        start=unix_to_date(unix_1)
        t_start =datetime.datetime(start[0],start[1], start[2], start[3], start[4])
        t_start=unix_1+60000
        # Define the end date
        unix_1+=43200000
        end=unix_to_date(unix_1)
        t_stop = datetime.datetime(end[0],end[1],end[2],end[3],end[4])
        t_stop=unix_1
        print(start," ---> ",end)
        result = api_v2.candles(symbol=pair, interval=bin_size,limit=limit, start=t_start, end=t_stop)
        lenr=len(result)-1
        while lenr>=0:
            price=result[lenr]
            new=pd.DataFrame({"UNIX" : [price[0]],"OPEN" : [price[1]],"HIGH" :[price[2]],"LOW" : [price[3]],"CLOSE" : [price[4]],"VOLUME":[price[5]]},index=[unix_date_string(price[0])])
            df=df.append(new)
            lenr-=1
        time.sleep(1)
    df.to_csv("/home/parsa/CRYPTO CHARTS/BTCUSD.csv")
def operator():
    df=pd.read_csv("/home/parsa/CRYPTO CHARTS/BTCUSD.csv")
    print(df)
    now=datetime.datetime.utcnow().timestamp()*1000
    now1=now-(now%43200000)
    x=df["UNIX"]
    x=x[len(x)-1]
    while x<now1:
        updater(df,x)
        x=df["UNIX"]
        x=x[len(x)-1]
        x+=43200000*10
    """
    now2=now-(now%600000)
    updater(df,now2)
    """
operator()
