# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import requests
import urllib.request
import time
from bs4 import BeautifulSoup
import pandas as pd
import os
a=open(os.devnull,'w')
import re
import datetime

input_file=pd.read_csv(r'/Users/tanmaysinghsidhu/Desktop/backup_p_files/Prices12_07_2019.csv')
lis=list(input_file['Identifier'])
start=time.time()
print(start)
counter=0
dic={}
#dic['RIC','Value'].update()
for i in lis:
    url='https://www.reuters.com/finance/stocks/overview/{}'.format(i)
    response=requests.get(url)

    soup=BeautifulSoup(response.text,"html.parser")
    #print(soup.prettify())
    #for i in range(len(soup.find_all('span'))):
    #    print(i,soup.find_all('span')[i])
    counter=counter+1
    try:
#        print(counter)
        actual_data=''.join(re.findall('\d*\.?\d+',soup.find_all('span')[14].text))
        print(counter,i,float(actual_data[1:]) if actual_data[0]=='.' else actual_data)
        input_file.loc[input_file['Identifier']==i,'Close_crawler']=float(actual_data[1:]) if actual_data[1:]=='.' else ''.join(actual_data)
        dic[i]=float(actual_data[1:]) if actual_data[1:]=='.' else ''.join(actual_data)
    except IndexError:
        input_file.loc[input_file['Identifier']==i,'Close_crawler']="Not found"
        dic[i]="Not found"
        print(counter,i,"Not found")
        continue
    
input_file['Pricedate_crawler']=datetime.datetime.now().strftime('%m/%d/%Y %H:%M')
input_file.to_csv("crawled_price_file1.csv")
end=time.time()
print("Total mins taken: ",(end-start)/60.0)
#print(soup.find('',class_='sectionQuoteDetailTop'))
#print(soup.find('span',class_='quoteCont').)
#
#print(soup.find("div").find('span').txt)
