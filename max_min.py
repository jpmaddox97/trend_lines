from profilehooks import profile
import pandas as pd
import datetime
from pull_datacb import getCBHistory

class MaxMin:

    def __init__(self, df: pd.DataFrame, period: int):
        self.period = period
        self.df     = df

    def update_cert(self, cert, index, df):
        #TODO: Implement ittertool module
        # Create certifying candle
        # count number of null values to see if is first row
        # print(index)
        null_count = 0
        for value in cert.values():
            if value == None:
                null_count += 1
        
        # update prev dictionary if all null values
        # if first row it will update with those values
        if null_count == 5:
            cert['t'] = index
            cert['o'] = float(df.loc[index]['Open'])
            cert['c'] = float(df.loc[index]['Close'])
            cert['h'] = float(df.loc[index]['High'])
            cert['l'] = float(df.loc[index]['Low'])
            
            return cert
        
        # update prev with new prev values (new prev is old curr)
        else:
            if index + datetime.timedelta(seconds=self.period):
                cert['t'] = (index)
                cert['o'] = float(df.loc[index]['Open'])
                cert['c'] = float(df.loc[index]['Close'])
                cert['h'] = float(df.loc[index]['High'])
                cert['l'] = float(df.loc[index]['Low'])
            
                return cert


    def update_curr(self, curr, index, df):
        # format datetime index
        # print(index)
        dt_object = (index - datetime.timedelta(seconds=self.period)).to_datetime64()
        
        if (df.index == dt_object).any():
            index -= datetime.timedelta(seconds=self.period)
            curr['t'] = (index)
            curr['o'] = float(df.loc[index]['Open'])
            curr['c'] = float(df.loc[index]['Close'])
            curr['h'] = float(df.loc[index]['High'])
            curr['l'] = float(df.loc[index]['Low'])
            
            # return the current candle dictionary
            return curr
        else:
            pass


    def update_prev(self, prev, index, df):
        # print(index)
        dt_object = (index - datetime.timedelta(seconds=2*self.period)).to_datetime64()

        if (df.index == dt_object).any():
            index -= datetime.timedelta(seconds=2*self.period)
            prev['t'] = (index)
            prev['o'] = float(df.loc[index]['Open'])
            prev['c'] = float(df.loc[index]['Close'])
            prev['h'] = float(df.loc[index]['High'])
            prev['l'] = float(df.loc[index]['Low'])
            
            # return the previous candle dictionary
            return prev
        else:
            pass


    def find_max(self, cert_candle, curr_candle, prev_candle):
        # get local max
        max_dict = {}
        if curr_candle and prev_candle:       
            if (curr_candle['c'] > cert_candle['c']) & (curr_candle['c'] > prev_candle['c']):
                max_dict = curr_candle
                return max_dict
            else:
                return None


    def find_min(self, cert_candle, curr_candle, prev_candle):   
        # get local min
        min_dict = {}
        if curr_candle and prev_candle:
            if (curr_candle['c'] < cert_candle['c']) & (curr_candle['c'] < prev_candle['c']):
                min_dict = curr_candle
                return min_dict
            else:
                return None


    def create_dataframe(self, df):
        """
        Create DataFrame with local minimums and maximums to create signals for double bottom creation
        Accepts DataFrames with columns formatted as 'Open', 'Close', 'High', 'Low'.
        More columns can be added.
        """
        
        # # store previous candle
        prev = {
            't' : None,
            'o' : None,
            'c' : None,
            'h' : None,
            'l' : None   
        }

        # store current candle
        curr = {
            't' : None,
            'o' : None,
            'c' : None,
            'h' : None,
            'l' : None 
        }

        # store certifying candle (certifies if the max/min is created)
        cert = {
            't' : None,
            'o' : None,
            'c' : None,
            'h' : None,
            'l' : None 
        }

        # Init lists for max and min events
        max_ = []
        min_ = []

        # Iterrate through dataframes indexes
        for index, row in df.iterrows():
            # index = pd.Timedelta(index)
            # print(index)
            cert_candle = self.update_cert(cert, index, df)
            curr_candle = self.update_curr(curr, index, df)
            prev_candle = self.update_prev(prev, index, df)
            
            mx = self.find_max(cert_candle, curr_candle, prev_candle)
            mn = self.find_min(cert_candle, curr_candle, prev_candle)

            # max stores close and high
            # min stores close and low
            if mx == None:
                max_.append(0)
            else:
                max_.append(1)

            if mn == None:
                min_.append(0)      
            else:
                min_.append(1)

        # Adding min/max events to original dataframe then creating a copy with columns for double bottom events
        max_min_df = pd.DataFrame({'Max': max_, 'Min': min_})
        max_min_df = max_min_df[['Max', 'Min']].shift(-1)
        max_min_df['Date'] = df.index
        max_min_df.set_index('Date', inplace=True)
        df_features = pd.concat([df, max_min_df], axis=1)
        
        return df_features

    
    def output(self):
        return self.create_dataframe(self.df)


if __name__ == '__main__':
    min        = 60
    five_min   = 300
    fiften_min = 900
    hour       = 3600
    four_hour  = 21600
    day        = 86400

    def get_maxmin(symbol, period, cycles):

        df      = getCBHistory(symbol, period, cycles=cycles)
        max_min = MaxMin(df, period)
        return max_min.output()


    dataframe = get_maxmin('BTC', min, 2)

    # minimums = dataframe.loc[dataframe['Min'] == 1]
    print(dataframe)