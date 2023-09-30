from bs4 import BeautifulSoup
import requests
import json
from yfinance_interaction import get_symbol_info, get_price_change_day, get_price_change_week, get_price_change_month
from datetime import datetime
from csv_interaction import write_to_csv


BASE_URL = "https://www.zacks.com/topics/zacks-consensus-estimate?page="
JSON_FILE = "data/earnings_data.json"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
}

def read_json():
    with open(JSON_FILE, "r") as json_file:
        data = json.load(json_file)
        return data

def write_json(data):
    with open(JSON_FILE, "w") as json_file:
        json.dump(data, json_file)

def get_datetime(date_string):
    date_format = "%B %d,%Y"
    date_obj = datetime.strptime(date_string, date_format)
    return date_obj

def scrape_content():
    for i in range(604,10000):
        current_url = BASE_URL+f'{i}'
        print(current_url)
        response = requests.get(current_url, headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find the div element with the class name "listitem"
        div_elements = soup.findAll('div', class_='listitem')

        # Extract the content within the div element
        for div_element in div_elements:
            try:
                if div_element:
                    byline = div_element.find('p', class_='byline')
                    publish_datetime = get_datetime(" ".join(byline.find('time').text.split()[-2:]))
                    teaser = div_element.find('p', class_='teaser').text
                    teaser_elems = teaser.split()
                    if teaser_elems[2] == 'delivered' and teaser_elems[3] == 'earnings':    
                        symbol = teaser_elems[1].strip("(").strip(")")
                        earnings = teaser_elems[8].strip("%")
                        revenue = teaser_elems[10][:-1][:-1]
                        market_cap, industry = get_symbol_info(symbol)
                        
                        # Day data
                        percentage_change_day = get_price_change_day(symbol, publish_datetime)
                        percentage_change_day = round(percentage_change_day,2)
                        data_structure = [[symbol, earnings, revenue, percentage_change_day, market_cap, industry]]
                        file_path_day = "data/earnings_data_1D.csv"
                        write_to_csv(file_path_day, data_structure)
                        
                        #Weekly data
                        percentage_change_week = get_price_change_week(symbol, publish_datetime)
                        if percentage_change_week != None:
                            percentage_change_week = round(percentage_change_week, 2)
                            data_structure = [[symbol, earnings, revenue, percentage_change_week, market_cap, industry]]
                            file_path_week = "data/earnings_data_1W.csv"
                            write_to_csv(file_path_week, data_structure)
                        
                        #Monthly data
                        percentage_change_month = get_price_change_month(symbol, publish_datetime)
                        if percentage_change_month != None:
                            percentage_change_month = round(percentage_change_month, 2)
                            data_structure = [[symbol, earnings, revenue, percentage_change_month, market_cap, industry]]
                            file_path_week = "data/earnings_data_1M.csv"
                            write_to_csv(file_path_week, data_structure)
                else:
                    print("Div element with class 'listitem' not found.")
            except Exception as e:
                print(e)


scrape_content()