import requests
from bs4 import BeautifulSoup
from time import sleep
import re
import pandas as pd
from tqdm import tqdm

def get_next_page(response):
    soup = BeautifulSoup(response.text, 'html.parser')
    pages = soup.find('div',class_='css-1xaekgw')
    try:
        return pages.find('a',class_='_1j1e08n0 _1j1e08n5').get('href')
    except Exception as e:
        print(e,url)
        return None

def generator():
    while True:
        yield

url = 'https://auto.drom.ru/japanese/all/'
dataset = []
file = open('dataset.json','w')
file.close()

for _ in tqdm(generator()):
    dataset = []
    resp = requests.get(url)
    soup = BeautifulSoup(resp.text, 'html.parser')
    for i in soup.find_all('a', class_="css-4zflqt"):
        l = i.find_all('div',recursive=False)
        card = l[1].text.split(',')
        try:
            brand = card[0].split(' ')[0]
            model = " ".join(card[0].split(' ')[1::])
        except:
            brand = None
            model = None
        
        try:
            year = int(card[1][0:5])
        except:
            year = None
        
        try:
            km = re.findall('\d+ \d+',card[5])
            if len(km)>0:
                km=km[0].split(' ')
                km=int(km[0])*1000+int(km[1])
            else:
                km=None
        except:
            km=None
        
        try:
            volume = float(re.search(r'\d.\d л',card[1]).group(0)[:3])
        except:
            volume = None
        
        try:
            power = int(re.match(r'\d+',re.search(r'\d+ л.с.',card[1]).group(0)).group(0))
        except:
            power = None
        
        try:
            engine_fuel = card[2]
        except:
            engine_fuel = None
        
        try:
            transmittion = card[3]
        except:
            transmittion = None
        
        try:
            drive = card[4]
        except:
            drive = None
        
        try:
            price = 0
            for ind,val in enumerate(l[2].span.text.split('\xa0')[:-1:][-1::-1]):
                price+=int(val)*1000**ind
        except:
            price = None
        
        dataset.append({
            'brand':brand,
            'model':model,
            'year':year,
            'fuel':engine_fuel,
            'transmition':transmittion,
            'driver':drive,
            'volume':volume,
            'power':power,
            'distance':km,
            'price':price
        })
        #print(brand,model,year,km,volume,power,engine_fuel,transmittion,drive)
    pd.DataFrame(dataset).to_json('dataset.json',orient='records',lines=True,mode='a')
    url = get_next_page(resp)
    #print(url)
    if url is None:
        break
    #sleep(4)