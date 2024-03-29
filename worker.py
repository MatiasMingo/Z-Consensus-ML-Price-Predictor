from ML_predictor import predict_LSTM
from bs4 import BeautifulSoup
import requests
import json
from lib.yfinance_interaction import get_symbol_info
from datetime import datetime
import pandas as pd
import joblib
import numpy as np
import telegram_interaction
import schedule
import time
import pytz




BASE_URL = "https://www.zacks.com/topics/zacks-consensus-estimate?page="
JSON_FILE = "data/new_earnings.json"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
}

"""
    SCRAPE NEW DATA
"""
def read_json():
    with open(JSON_FILE, "r") as json_file:
        data = json.load(json_file)
        return data

def write_new_data_json(data):
    previous_dict = read_json()
    new_data_list = []
    for new_earnings_dict in data:
        publish_datetime = new_earnings_dict['Datetime']
        symbol = new_earnings_dict['Ticker']
        datetime_list = previous_dict.get(publish_datetime, None)
        if datetime_list != None:
            it_exists = False
            for ticker_dict in datetime_list:
                if symbol == ticker_dict['Ticker']:
                    it_exists = True
            if not it_exists:
                datetime_list.append(new_earnings_dict)
                new_data_list.append(new_earnings_dict)
        else:
            previous_dict[publish_datetime] = [new_earnings_dict]
            if len(new_data_list) == 0:
                new_data_list = [new_earnings_dict]
            else:
                new_data_list.append(new_earnings_dict)
    with open(JSON_FILE, "w") as json_file:
        json.dump(previous_dict, json_file)
    return new_data_list

def get_datetime(date_string):
    date_format = "%B %d,%Y"
    date_obj = datetime.strptime(date_string, date_format)
    return date_obj

def scrape_content():
    new_earnings_list = []
    for i in range(1,3):
        current_url = BASE_URL+f'{i}'
        print(current_url)
        response = requests.get(current_url, headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        div_elements = soup.findAll('div', class_='listitem')
        for div_element in div_elements:
            try:
                if div_element:
                    byline = div_element.find('p', class_='byline')
                    publish_datetime = get_datetime(" ".join(byline.find('time').text.split()[-2:])).strftime("%Y-%m-%d")
                    teaser = div_element.find('p', class_='teaser').text
                    teaser_elems = teaser.split()
                    space_count = 0
                    try:
                        index = teaser.index('(')
                        single_chracters_list = list(teaser)
                        space_count = single_chracters_list[0:index].count(' ')-1
                    except ValueError:
                        continue
                    if teaser_elems[2+space_count] == 'delivered' and teaser_elems[3+ space_count] == 'earnings':    
                        symbol = teaser_elems[1+space_count].strip("(").strip(")")
                        earnings = float(teaser_elems[8+space_count].strip("%"))
                        revenue = float(teaser_elems[10+space_count][:-1][:-1])
                        market_cap, industry = get_symbol_info(symbol)
                        new_earnings_list.append({'Ticker': symbol, 'Earnings': earnings, 'Revenue': revenue, 'Market cap': market_cap, 
                                                  'Industry':industry, 'Datetime': publish_datetime})
            except Exception as e:
                print(e)
    new_data_list = write_new_data_json(new_earnings_list)
    return new_data_list


"""
    DATA PREPROCESSING
"""

def get_industries_dict():
    with open("data/industries_dict.json", "r") as json_file:
        data = json.load(json_file)
        return data   

def load_scalers():
    scaler_1D = joblib.load("scalers/scaler_earnings_1D.pkl")
    scaler_1W = joblib.load("scalers/scaler_earnings_1W.pkl")
    scaler_1M = joblib.load("scalers/scaler_earnings_1M.pkl")
    return scaler_1D, scaler_1W, scaler_1M

def data_preprocessing(new_data_list):
    new_data = pd.DataFrame(new_data_list)
    industries_dict = get_industries_dict()
    for index in new_data.index:
        # Perform some operation to change the value (e.g., double the value)
        new_value = industries_dict[new_data.loc[index, 'Industry']]
        # Update the DataFrame with the new value
        new_data.loc[index, 'Industry'] = new_value
    numerical_columns = ['Earnings', 'Revenue', 'Market cap']
    for col in numerical_columns:
        new_data[col] = new_data[col].replace(',', '', regex=True).astype(float)

    new_data = new_data.drop(columns=['Datetime'])
    new_data['Percentage change'] = 0
    non_numerical_columns = new_data['Ticker']
    new_data.drop(columns=['Ticker'], inplace=True)

    industry_column = new_data["Industry"]
    new_data = new_data.drop(columns=['Industry'])
    
    #reorder data for scaler
    desired_order = ['Earnings', 'Revenue', 'Percentage change', 'Market cap']
    new_data = new_data[desired_order]
    #scale the data for 1D, 1W and 1M
    scaler_1D, scaler_1W, scaler_1M = load_scalers()
    scaled_data_1D = scaler_1D.transform(new_data)
    scaled_data_1W = scaler_1W.transform(new_data)
    scaled_data_1M = scaler_1M.transform(new_data)

    numerical_columns = ['Earnings', 'Revenue', 'Percentage change', 'Market cap']


    new_data_1D = pd.DataFrame(scaled_data_1D, columns=numerical_columns)
    new_data_1W = pd.DataFrame(scaled_data_1W, columns=numerical_columns)
    new_data_1M = pd.DataFrame(scaled_data_1M, columns=numerical_columns)

    new_data_1D = new_data_1D.drop(columns=['Percentage change'])
    new_data_1W = new_data_1W.drop(columns=['Percentage change'])
    new_data_1M = new_data_1M.drop(columns=['Percentage change'])

    new_data_1D = pd.concat([new_data_1D, industry_column], axis=1)
    new_data_1W = pd.concat([new_data_1W, industry_column], axis=1)
    new_data_1M = pd.concat([new_data_1M, industry_column], axis=1)

    X_new_1D= new_data_1D.to_numpy()
    new_x_1D = []
    for vert_new in X_new_1D:
        new_x_1D.append(vert_new[:6])
    X_new_1D = pd.DataFrame(data=np.array(new_x_1D, dtype=np.float32), columns= ['Earnings', 'Revenue', 'Market cap', 'Industry'])
    X_new_1D = X_new_1D.iloc[:, [True, True, True, True]]

    X_new_1W= new_data_1W.to_numpy()
    new_x_1W = []
    for vert_new in X_new_1W:
        new_x_1W.append(vert_new[:6])
    X_new_1W = pd.DataFrame(data=np.array(new_x_1W, dtype=np.float32), columns= ['Earnings', 'Revenue', 'Market cap', 'Industry'])
    X_new_1W = X_new_1W.iloc[:, [True, True, True, True]]

    X_new_1M= new_data_1M.to_numpy()
    new_x_1M = []
    for vert_new in X_new_1M:
        new_x_1M.append(vert_new[:6])
    X_new_1M = pd.DataFrame(data=np.array(new_x_1M, dtype=np.float32), columns= ['Earnings', 'Revenue', 'Market cap', 'Industry'])
    X_new_1M = X_new_1M.iloc[:, [True, True, True, True]]
    
    X_new_1D = pd.concat([X_new_1D, non_numerical_columns], axis=1)
    X_new_1W = pd.concat([X_new_1W, non_numerical_columns], axis=1)
    X_new_1M = pd.concat([X_new_1M, non_numerical_columns], axis=1)
    
    return X_new_1D, X_new_1W, X_new_1M


"""
    PUTTING IT ALL TOGETHER
"""

def predict_new_data():
    new_data_list = scrape_content()
    if len(new_data_list) > 0:
        x_1D, x_1W, x_1M = data_preprocessing(new_data_list)
        prediction_1D, prediction_1W, prediction_1M = predict_LSTM(x_1D, x_1W, x_1M)
        x_1D['Percentage change'] = prediction_1D['Prediction_1D']
        x_1W['Percentage change'] = prediction_1W['Prediction_1W']
        x_1M['Percentage change'] = prediction_1M['Prediction_1M']
        
        print(x_1D)
        print(x_1W)
        print(x_1M)
        
        x_1D_industry_column = x_1D["Industry"]
        x_1D = x_1D.drop(columns=['Industry'])

        x_1W_industry_column = x_1W["Industry"]
        x_1W = x_1W.drop(columns=['Industry'])

        x_1M_industry_column = x_1M["Industry"]
        x_1M = x_1M.drop(columns=['Industry'])
    
        #reorder for scaling
        desired_order = ['Earnings', 'Revenue', 'Percentage change', 'Market cap']
        
        x_1D = x_1D[desired_order]
        x_1W = x_1W[desired_order]
        x_1M = x_1M[desired_order]
        
        #scale the data for 1D, 1W and 1M
        scaler_1D, scaler_1W, scaler_1M = load_scalers()
        
        scaled_data_1D = scaler_1D.inverse_transform(x_1D)
        scaled_data_1W = scaler_1W.inverse_transform(x_1W)
        scaled_data_1M = scaler_1M.inverse_transform(x_1M)
        
        # After scaling the data, create a new DataFrame with the desired order of columns
        scaled_data_1D = pd.DataFrame(scaled_data_1D, columns=desired_order)

        # Add the 'Industry' column back to the DataFrame
        scaled_data_1D['Industry'] = x_1D_industry_column

        # Do the same for scaled_data_1W and scaled_data_1M
        scaled_data_1W = pd.DataFrame(scaled_data_1W, columns=desired_order)
        scaled_data_1W['Industry'] = x_1W_industry_column

        scaled_data_1M = pd.DataFrame(scaled_data_1M, columns=desired_order)
        scaled_data_1M['Industry'] = x_1M_industry_column

        scaled_data_1D['Ticker'] = prediction_1D['Ticker']
        scaled_data_1W['Ticker'] = prediction_1W['Ticker']
        scaled_data_1M['Ticker'] = prediction_1M['Ticker']
        
        print(scaled_data_1D)
        print(scaled_data_1W)
        print(scaled_data_1M)
        propagate_notifications(scaled_data_1D, scaled_data_1W, scaled_data_1M)

def propagate_notifications(df_1D, df_1W, df_1M):
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime("%d/%m/%Y - %H:%M")
    notification = f"🛸🛸🛸🍇  New predictions  🍇🛸🛸🛸\n{formatted_datetime}\n\nTicker  1D  1W  1M \n\n"
    for index, row in df_1D.iterrows():
        notification += f"{row['Ticker']}:  {round(row['Percentage change'],2)}%  {round(float(df_1W[df_1W['Ticker'] == row['Ticker']]['Percentage change']),2)}%  {round(float(df_1M[df_1M['Ticker'] == row['Ticker']]['Percentage change']),2)}%\n\n"
    telegram_interaction.send(notification)   

def schedule_task():
    # Define your time zone (Eastern Time)
    eastern = pytz.timezone('US/Eastern')

    # Get the current time in the specified time zone
    current_time = datetime.now(eastern)

    # Define the time ranges when the task should run
    morning_start_time = current_time.replace(hour=5, minute=0, second=0, microsecond=0)
    morning_end_time = current_time.replace(hour=13, minute=30, second=0, microsecond=0)
    evening_start_time = current_time.replace(hour=18, minute=0, second=0, microsecond=0)
    evening_end_time = current_time.replace(hour=20, minute=30, second=0, microsecond=0)

    # Schedule the task to run every 15 minutes
    while True:
        if morning_start_time <= current_time <= morning_end_time or \
           evening_start_time <= current_time <= evening_end_time:
            predict_new_data()
        time.sleep(900)  # 15 minutes in seconds
        current_time = datetime.now(eastern)

if __name__ == "__main__":
    schedule_task()
