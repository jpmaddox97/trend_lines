from pull_datacb import getCBHistory, getLastRow
from datetime import timedelta, datetime
from dotenv import load_dotenv
from max_min import MaxMin
from slope import slope, slope_tick
import pandas as pd
import cbpro
import sys
import os

# While working within the sandbox you will only be able to trade BTC-USD and ETH-BTC
# When connected to the actual cbpro clien you will be able to trade all pairs available on coinbase

load_dotenv()
cb_key    = os.getenv("CBP_SB_KEY")
cb_pass   = os.getenv("CBP_SB_PHRASE")
cb_secret = os.getenv("CBP_SB_SECRET")

# A websocket class to get streaming data from the coinbase pr API
# Currently testing on the sandbox
class TextWebsocketClient(cbpro.WebsocketClient):
    
    def __init__(self, url="wss://ws-feed.pro.coinbase.com", products=None, message_type="subscribe", mongo_collection=None, should_print=True, auth=False, api_key="", api_secret="", api_passphrase="", channels: str=None, period: int=None, symbol: str=None):
        super().__init__(url, products, message_type, mongo_collection, should_print, auth, api_key, api_secret, api_passphrase, channels)
        try:
            self.period = int(period)
        except:
            print("You must enter an integer")
        self.symbol = symbol.upper()
        self.order_book = []


    def on_open(self):
        # Open socket connect with sandbox init message counter
        self.client = cbpro.AuthenticatedClient(
            cb_key,
            cb_secret,
            cb_pass,  
            api_url=url
        )
        print('websocket open')
        self.url           = "wss://ws-feed-public.sandbox.exchange.coinbase.com"
        self.message_count = 0
        self.dataframe     = getCBHistory(self.symbol, self.period, cycles=4)
        


    def updater(self, symbol: str, period: int):
        row       = getLastRow(symbol, period)
        row       = row[::-1]
        row_time  = pd.to_datetime(row.index[-1])
        ltime     = pd.to_datetime(self.dataframe.index[-1])
        next_time = ltime + timedelta(seconds=period)
        

        # print("updater")

        if row_time == ltime:
            pass
        elif row_time == next_time:

            print("Added row")
            return row
        else:
            pass


    def get_maxmin(self, df: pd.DataFrame, period: int):
        max_min = MaxMin(df, period)
        return max_min.output()

        
    def on_message(self,msg):
        print("message")
        self.message_count += 1
        #set message type to json dictionary
        msg_type = msg.get('type',None)


        if msg_type == 'ticker':
            time_val    = msg.get('time',('-'*27))
            price_val   = msg.get('price',None)
            ts          = pd.to_datetime(time_val)
            self.length = len(self.dataframe)


            if price_val is not None:
                price_val = float(price_val)


            try:
                row = self.updater(self.symbol, self.period)
                
                if row is None:
                    print(self.dataframe.tail(1))
                    pass
                else:
                    # Get new index
                    # self.df_index = int(len(self.dataframe))
                    # print(row)

                    # Add new row
                    # new_row = pd.Series(row.iloc[0], name=row.loc['Date'])
                    self.dataframe = self.dataframe.append(row)
                    print(self.dataframe.tail(5))
                    # self.dataframe.set_index('Date', inplace=True)
                    max_min   = self.get_maxmin(self.dataframe, self.period)
                    sl = slope(max_min)
                    slt = slope_tick(max_min)
                    print(f'Slope of minimums: {sl}')
                    print(f'Slope of tick: {slt}')

                    if sl < slt:
                        print('Price greater than trend line')
                    else:
                        print('Price is lower than trend line')

            except KeyboardInterrupt:
                self.on_close()

            # print("message")

        
    def on_close(self):
        self.dataframe.to_csv('test_df.csv')
        # self.algo_df.to_csv('algo_df.csv')
        print(f"<---Websocket connection closed--->\n\tTotal messages: {self.message_count}")


if __name__ == '__main__':
    url='https://api-public.sandbox.pro.coinbase.com'
    # url='wss://ws-feed.exchange.coinbase.com'

    stream = TextWebsocketClient(products=['BTC-USD'],channels=['ticker'], url=url, period=60, symbol='BTC')
    stream.start()

    try:
        while True:
            stream
    except KeyboardInterrupt:
        stream.close()

    if stream.error:
        sys.exit(1)
    else:
        sys.exit(0)