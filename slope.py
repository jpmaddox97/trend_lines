from max_min import MaxMin
import pandas as pd

def get_maxmin(df: pd.DataFrame, period: int):
    max_min = MaxMin(df, period)
    return max_min.output()

def slope(dataframe):
    # Get slope of last two minimum values
    minimums = dataframe.loc[dataframe['Min'] == 1]
    last_four = minimums.tail(2)
    last_four.reset_index(inplace=True)

    first_point  = []
    second_point = []
    for i, row in last_four.iterrows():
        if i == 0:
            dt = last_four.iloc[i]['Date']
            first_point.append(dt)
            price = last_four.iloc[i]['Close']
            first_point.append(price)
        if i == 1:
            dt = last_four.iloc[i]['Date']
            second_point.append(dt)
            price = last_four.iloc[i]['Close']
            second_point.append(price)

    change_y = second_point[1] - first_point[1]
    change_x = (second_point[0] - first_point[0]).total_seconds()

    return change_y/change_x

def slope_tick(dataframe):
    # Get slope of last two minimum values
    minimums = dataframe.loc[dataframe['Min'] == 1]
    last_four = minimums.tail(1)
    last_four.reset_index(inplace=True)

    last_call = dataframe.tail(1)
    first_point  = []
 
    dt = last_four.iloc[0]['Date']
    # first_point.append(dt)
    price = last_four.iloc[0]['Close']
    # first_point.append(price)

    dt_2    = last_call.iloc[0].name
    price_2 = last_call.iloc[0]['Close']
    print(f'Change in dt_2: {dt_2}')
    print(f'Change in price_2: {price_2}')

    change_y = price_2 - price
    change_x = (dt_2 - dt).total_seconds()
    print(f'Change in y: {change_y}')
    print(f'Change in x: {change_x}')

    return change_y/change_x