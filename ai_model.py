import ccxt
import pandas as pd
import datetime as dt
from prophet import Prophet


class live_predict:
    def __init__(self):
        self.exchange = ccxt.binance()
        self.symbol = 'BTC/USDT'
        self.timeframe = '1m'
        self.limit = 1000  
        self.model = Prophet()
        self.historical_data=None
        self.dataset=None
        self.latest=None
        self.latest_price=0
        self.Num_of_predict=0
        self.Num_of_secs=300
        self.future=None
        self.forecast=0

    def make_data_and_train(self):
        if self.dataset!=None:
            if len(self.dataset)>=86400:
                del self.dataset[:self.Num_of_secs]
            
        if self.dataset==None:
            self.dataset= self.exchange.fetch_ohlcv( self.symbol, self.timeframe, limit= self.limit)
            self.historical_data=self.dataset 
            self.latest=self.historical_data[-1][0]
            self.latest+=1000
        else:
            self.model = Prophet()
            historical_data= self.exchange.fetch_ohlcv( self.symbol,  self.timeframe,since=self.latest, limit=None)
            self.dataset+=historical_data
            self.historical_data=self.dataset 
            self.latest=self.historical_data[-1][0]
            self.latest+=1000

        df = pd.DataFrame( self.historical_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df.drop(columns=['open', 'high', 'low', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        df_5min = df.resample('1s').agg({'close': 'last'}).dropna()
        df_prophet = df_5min.reset_index().rename(columns={'timestamp': 'ds', 'close': 'y'})
        df_prophet['y'] = df_prophet['y'].astype(float)
        self.df_prophet=df_prophet
        self.model.fit(self.df_prophet)

    
    def live_Price_Display(self):
        ticker = self.exchange.fetch_ticker(self.symbol)
    # Get the datetime and price from the ticker
        datetime1 = ticker['timestamp']
        d=datetime1 / 1000
        datetime_obj = dt.datetime.fromtimestamp(d)
        datetime_str = datetime_obj.strftime('%Y-%m-%d %H:%M:%S')
        d1=(datetime1+60000)/1000
        datetime_obj1 = dt.datetime.fromtimestamp(d1)
        datetime_str1 = datetime_obj1.strftime('%Y-%m-%d %H:%M:%S')
        invalue=self.get_user_predict(datetime_str1)
        invalue= round(invalue,2)
        price = ticker['last']
        high_price=ticker['high']
        low_price=ticker['low']
        bid=ticker['bid']
        bidVolume=ticker['bidVolume']
        ask=ticker['ask']
        askVolume=ticker['askVolume']
        vwap=ticker['vwap']
        open1=ticker['open']
        close1=ticker['close']
        previousClose=ticker['previousClose']
        change=ticker['change']
        percentage=ticker['percentage']
        average=ticker['average']
        baseVolume=ticker['baseVolume']
        quoteVolume=ticker['quoteVolume']
        return datetime_str,price,high_price,low_price,bid,bidVolume,ask,askVolume,vwap,open1,close1,previousClose,change,percentage,average,baseVolume,quoteVolume,invalue,datetime_str1
        
    def get_user_predict(self,date_string):       
        date_format = '%Y-%m-%d %H:%M:%S'
        datetime_obj = pd.to_datetime(date_string, format=date_format)
        new_df = pd.DataFrame({'ds': [datetime_obj]})
        forecast = self.model.predict(new_df)
        #  Print the predicted value for the specified date
        predicted_value = forecast.loc[0, 'yhat']
        print(f'Predicted value for {date_string}: {predicted_value:.2f}')
        return predicted_value
    

# a1=live_predict()
# a1.make_data_and_train()
# f=1679420411606#a1.exchange.fetch_ticker(a1.symbol)['timestamp']
# print(f)
# f+=10000
# f1=int(f/1000)
# f2=dt.datetime.fromtimestamp(f1)
# #datetime_obj = pd.to_datetime(f2, unit='ms')
# print(f2)   