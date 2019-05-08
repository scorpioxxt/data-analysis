#!/usr/bin/env python 
# -*- coding:utf-8 -*-
import io
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import json
import random

from multiprocessing.pool import Pool



import pymysql
import pandas as pd
import requests

from bs4 import BeautifulSoup

import time

from lxml import etree
import re

df = pd.read_csv("process.csv")
print df.dtypes
df.info()
name = df.title
diqu = df.diqu
town = df.town
dizhi = []

for i in range(len(name)):
    dizhi.append(diqu[i]+town[i]+name[i])

def getlocation(name):
    url = 'http://api.map.baidu.com/geocoder/v2/'

    output = 'json'

    city = '武汉市'
    ak = 'WwGOnc3qiQ4ikEceDF0YhvknDXQ2CeV2'

    address = name

    uri = url + '?' + 'address=' + address + '&city='+ city + '&output=' + output + '&ak=' + ak

    print uri

    resp = requests.get(uri)

    text = json.loads(resp.text)

    print text

    if 'result' in text:

        lat = text['result']['location']['lat']

        lng = text['result']['location']['lng']

    else:
        lat = 0
        lng = 0

    return lat, lng

lat=[]
lag=[]
p=0
i=0
for i in range(2259):
    a, b = getlocation(dizhi[i])
    lat.append(a)
    lag.append(b)
    p += 1
    print a,b,p
    time.sleep(2)
df1 =pd.DataFrame(dict(lat = lat,lag = lag))



df4=pd.concat([df,df1],axis=1)
df4.to_csv("wuhanall.csv",encoding="utf_8_sig",index=False)











