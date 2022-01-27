import pandas as pd
from datetime import datetime, timedelta
import cbpro

# Import client and init dictionary of granularity
c    = cbpro.PublicClient()

def _concat(dataframe, data):
    return pd.concat([dataframe, data]) 


def _make_df(df):
    columns = ['time', 'Low', 'High', 'Open', 'Close', 'volume']
    return pd.DataFrame(df, columns=columns)


def getCBHistory(symbol: str, period: int, cycles: int = 5):
    '''
    Returns historical OHLCV price data from coinbase.

        Parameters: 
                symbol (str): The symbol of the cryptocurrency
                period (int): Granularity of data in seconds. 
                    Accepts one minute, five minutes, 15 minutes, 
                    one hour, four hour and one day

                    min   = 60
                    5min  = 300
                    15min = 900
                    hour  = 3600
                    4hour = 21600
                    day   = 86400   

                cycles (int): data is returned in groups of 300 data points. 
                    If you want more than 300 points of data you will need 
                    to increase the number of cycles. 
                    Default is 1 cycle.

        Returns:
                data_df (dataframe): A pandas dataframe
    '''
    # Initialize count
    count = 1

    # Set initial start and end time
    timeEnd   = datetime.now()
    delta     = timedelta(seconds = int(period))
    timeStart = timeEnd - (300*delta)

    # Iterate through cycles and return dataframe
    # if cycles != 1:
    while count <= cycles:
        if count == 1:
            start     = timeStart.isoformat()
            end       = timeEnd.isoformat()
            dataframe = c.get_product_historic_rates(f'{symbol.upper()}-USD', start, end, period)
            dataframe = _make_df(dataframe)
            df        = dataframe

        else:
            # timeEnd   = timeStart - (delta)
            timeEnd   = timeStart
            timeStart = timeEnd - (300*delta)
            end       = timeEnd.isoformat()
            start     = timeStart.isoformat()
            data      = c.get_product_historic_rates(f'{symbol.upper()}-USD', start, end, period)
            data      = _make_df(data)
            if data.empty:
                break
            df        = _concat(dataframe, data)   
            dataframe = df
        
        count += 1

    # Check if symbol is listed and format dataframe
    if df.empty:
        return 'Error: That symbol is not listed on Coinbase'
    else:
        df['Date'] = pd.to_datetime(df['time'], unit='s')
        data_df    = df[['Date', 'Open', 'High', 'Low', 'Close']]
        data_df    = data_df[::-1].reset_index()
        data_df.drop('index', axis=1, inplace=True)
        data_df    = data_df.set_index(['Date'])

        return data_df

def getLastRow(symbol: str, period: int):
    '''
    Returns most recent candle

        Parameters: 
                symbol (str): The symbol of the cryptocurrency
                period (int): Granularity of data in seconds. 
                    Accepts one minute, five minutes, 15 minutes, 
                    one hour, four hour and one day.

                    Min   = 60,
                    5Min  = 300,
                    15Min = 900,
                    Hour  = 3600,
                    4Hour = 21600,
                    Day   = 86400,  

        Returns:
                data_df (dataframe): A pandas dataframe containing one row
    '''    
    # Time formatting
    delta     = timedelta(seconds = int(period))
    timeEnd   = datetime.now() - delta
    timeStart = timeEnd - (delta)
    start     = timeStart.isoformat()
    end       = timeEnd.isoformat()

    # Row formatting
    row         = c.get_product_historic_rates(f'{symbol.upper()}-USD', start, end, period)
    row         = _make_df(row)
    row['Date'] = pd.to_datetime(row['time'], unit='s')
    row         = row[['Date', 'Open', 'High', 'Low', 'Close']]
    row         = row.reset_index()
    row.drop('index', axis=1, inplace=True)
    row         = row.set_index(['Date'])
    # row         = row.set_index(['Date'])

    return row
