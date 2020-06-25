#In The Name Of "THE BEST"

"""
>>> Libraries :
"""

import bitfinex
import datetime
import time
import os
import pandas as pd
from subprocess import PIPE,Popen

"""
>>> Description :
"""

"""
    This is an updator for the price of bitcoin
    in dollar.
    It uses BITFINEX API for gettng data and stores
    the data in a folder called BTC_USD_UPDATOR.
    For speed and simplicity the data is devided into 
    smaller parts which all are named by their
    starting and ending UNIX timestamp.
    It is also built to be fast so
    is uses and integration with pypy3 and python3.
    It is designed to be used as a cronjob.
    Wish you be satisfied ...

"""

"""
>>> pypy3 and python3 integration tools :
"""

#D these are used in the communication functions

def create_byte(l_things):
  final=b""
  next_line="\n"
  for i in l_things:
    final+=i.encode('utf-8')+next_line.encode('utf-8')
  return final
def analyze_byte(x):
  x=x.decode('utf-8')
  x=x.replace("\n","@")
  x=x.split("@")
  x=x[:-1]
  l=[]
  for i in x:
    if i.isnumeric():
      l.append(int(i))
    else:
      l.append(i)
  return l

#F communication_with_input(
# environment --> pypy3 or python3
# ,path --> /home/parsa/BTC_USD/folder_builder.py
# ,your_input --> a list containing your inputs
# ) --> return (a list containing the answers)

def communication_with_input(environment,path,your_input):
  process = Popen([environment,path], stdin=PIPE, stdout=PIPE)
  process.stdin.write(create_byte(your_input))                                                                                                      
  x=process.communicate()[0]
  return analyze_byte(x)

#F communication(
# environment --> pypy3 or python3
# ,path --> /home/parsa/BTC_USD/folder_builder.py
# ) --> return (a list containing the answers)

def communication(environment,path):
  process = Popen([environment,path], stdin=PIPE, stdout=PIPE)                                                                                                        
  x=process.communicate()[0]
  return analyze_byte(x)

"""
>>> Code :
"""

#F unix_date_string(unix) --> return datetime

def unix_date_string(unix):
    ts = int(unix)/1000
    # if you encounter a "year is out of range" error the timestamp
    # may be in milliseconds, try `ts /= 1000` in that case
    return str(datetime.datetime.utcfromtimestamp(ts).strftime("%Y"))+" , "+str(datetime.datetime.utcfromtimestamp(ts).strftime("%m"))+" , "+str(datetime.datetime.utcfromtimestamp(ts).strftime("%d"))+" , "+str(datetime.datetime.utcfromtimestamp(ts).strftime("%H"))+" , "+str(datetime.datetime.utcfromtimestamp(ts).strftime("%M"))

#F find_gap(start,end) --> return gap/False

def find_gap(start,end):
    #D Some useful times in millisecond:
    # Minute --> 60000
    # 5 Minutes --> 300000
    # 10 Minutes --> 600000
    # Hour --> 3600000
    # 6 Hours --> 21600000
    # 12 Hours --> 43200000
    # Day --> 86400000
    # 10 Days --> 864000000
    # Month --> 2592000000
    l_gaps=[
        43200000, # 12 Hours
        21600000, # 6 Hours
        3600000, # Hour
        600000, # 10 Minutes
        300000 # 5 Minutes
    ]
    for i in l_gaps:
        if end-start>=i:
            return i
    return False

#F get_data(start,end) --> return DataFrame/False
#D it gets the data for that period and return a DataFrame

def get_data(start,end):
    df=pd.DataFrame(
        {
            "UNIX" : [] ,
            "OPEN" : [] ,
            "HIGH" : [] ,
            "LOW" : [] ,
            "CLOSE" : [] ,
            "VOLUME" : []
        }
    )
    api_v2 = bitfinex.bitfinex_v2.api_v2()
    # Define query parameters
    pair = 'btcusd' # Currency pair of interest
    bin_size = '1m' # This will return minute data
    limit = 1000    # We want the maximum of 1000 data points 
    # Define the start date
    while find_gap(start,end)!=False:
        print(end-start)
        gap=find_gap(start,end)
        print("GAP -->",gap)
        print(unix_date_string(start),"-->",unix_date_string(start+gap))
        try:
            result = api_v2.candles(
                symbol=pair,
                interval=bin_size,
                limit=limit, 
                start=start+60000, 
                end=start+gap
            )
            len_result=len(result)-1
            while len_result>=0:
                l_result=[]
                for i in range(5):
                    l_result.append(result[len_result-i])
                new=pd.DataFrame(
                    {
                        "UNIX" : [
                            l_result[0][0],
                            l_result[1][0],
                            l_result[2][0],
                            l_result[3][0],
                            l_result[4][0],
                        ],
                        "OPEN" : [
                            l_result[0][1],
                            l_result[1][1],
                            l_result[2][1],
                            l_result[3][1],
                            l_result[4][1],
                        ],
                        "HIGH" :[
                            l_result[0][2],
                            l_result[1][2],
                            l_result[2][2],
                            l_result[3][2],
                            l_result[4][2],
                        ],
                        "LOW" : [
                            l_result[0][3],
                            l_result[1][3],
                            l_result[2][3],
                            l_result[3][3],
                            l_result[4][3],
                        ],
                        "CLOSE" : [
                            l_result[0][4],
                            l_result[1][4],
                            l_result[2][4],
                            l_result[3][4],
                            l_result[4][4],
                        ],
                        "VOLUME":[
                            l_result[0][5],
                            l_result[1][5],
                            l_result[2][5],
                            l_result[3][5],
                            l_result[4][5],
                        ]
                    }
                )
                df=df.append(new,ignore_index=True)
                len_result-=5
            time.sleep(1)
            print("DONE")
        except:
            print("<<< SOMETHING WENT WRONG >>>")
            return False
        start+=gap
    return df 

#F path_builder(start,end) --> return a name 

def path_builder(start,end):
    return "/home/parsa/BTC_USD/"+str(start)+","+str(end)+","+unix_date_string(start)+","+unix_date_string(end)

#F path_reader(path) --> return list

def path_reader(path):
    path=path.split("/")[-1]
    path=path.split(",")
    return int(path[0]),int(path[1])

#L firm(x,y) --> x=x-x%y

firm = lambda x,y : x-x%y

#F UOHLCV_builder(path)
#D UOHLCV= UNIX-OPEN-HIGH-LOW-CLOSE-VOLUME
#D It makes an empty UOHLCV file in that path

def UOHLCV_builder(path):
    start,end=path_reader(path)
    """
    api_v2 = bitfinex.bitfinex_v2.api_v2()
    # Define query parameters
    pair = 'btcusd' # Currency pair of interest
    bin_size = '1m' # This will return minute data
    limit = 1000    # We want the maximum of 1000 data points 
    # Define the start date
    data = api_v2.candles(
        symbol=pair,
        interval=bin_size,
        limit=limit, 
        start=start, 
        end=start+60000
    )[1]
    if os.path.isfile(path):
        os.remove(path)
    df=pd.DataFrame(
        {
            "UNIX" : [data[0]] ,
            "OPEN" : [data[1]] ,
            "HIGH" : [data[2]] ,
            "LOW" : [data[3]] ,
            "CLOSE" : [data[4]] ,
            "VOLUME" : [data[5]]
        }
    )
    """
    if os.path.isfile(path):
        os.remove(path)
    df=pd.DataFrame(
        {
            "UNIX" : [] ,
            "OPEN" : [] ,
            "HIGH" : [] ,
            "LOW" : [] ,
            "CLOSE" : [] ,
            "VOLUME" : []

        }
    )
    df.to_csv(path)
    return df
    
#F operator()
#D finishes the job

def operator():

    #D Some useful times in millisecond:
    # Minute --> 60000
    # 5 Minutes --> 300000
    # 10 Minutes --> 600000
    # Hour --> 3600000
    # 6 Hours --> 21600000
    # 12 Hours --> 43200000
    # Day --> 86400000
    # 10 Days --> 864000000
    # Month --> 2592000000

    file=open("/home/parsa/BTC_USD/last.txt","r")
    first_start=int(file.read())
    file.close()
    end=datetime.datetime.utcnow().timestamp()*1000
    end-=end%300000
    start=first_start+60000

    #D finding the df

    while start<=end:

        # Finding the path and df

        if firm(start,86400000) > firm(first_start,86400000): # new time
            path=path_builder(firm(start,86400000),firm(start+86400000,86400000))+".csv"
            df=UOHLCV_builder(path)
            first=path_reader(path)[0]
        else:
            path=path_builder(firm(first_start,86400000),firm(first_start+86400000,86400000))+".csv"
            df=pd.read_csv(path)
            first=path_reader(path)[0]+60000
        print("start :",path_reader(path)[0])
        print("end :",path_reader(path)[1])

        # Getting the data and storing it

        df=df.append(get_data(first,path_reader(path)[1]),ignore_index=True)
        df.to_csv(path)
        file=open("/home/parsa/BTC_USD/last.txt","w")
        file.write(str(path_reader(path)[1]-60000))
        file.close()

        # Updating the variables

        file=open("/home/parsa/BTC_USD/last.txt","r")
        first_start=int(file.read())
        file.close()
        start=first_start+60000

operator()
