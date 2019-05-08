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

import time

from lxml import etree
import re



import requests

from bs4 import BeautifulSoup
from bs4 import UnicodeDammit

hds=[{'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'},\

    {'User-Agent':'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.12 Safari/535.11'},\

    {'User-Agent':'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)'},\

    {'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:34.0) Gecko/20100101 Firefox/34.0'},\

    {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/44.0.2403.89 Chrome/44.0.2403.89 Safari/537.36'},\

    {'User-Agent':'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\

    {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\

    {'User-Agent':'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0'},\

    {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\

    {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\

    {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11'},\

    {'User-Agent':'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11'},\

    {'User-Agent':'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11'}]


def getlocation(name):
    url = 'http://api.map.baidu.com/geocoder/v2/'

    output = 'json'

    ak = 'V4jViWqvBdGgS3yM1B5VNOhfqRqwMZx6'

    address = name

    uri = url + '?' + 'address=' + address + '&output=' + output + '&ak=' + ak

    resp = requests.get(uri,headers=hds[random.randint(0,len(hds)-1)])

    text = json.loads(resp.text)

    if 'result' in text:

        lat = text['result']['location']['lat']

        lng = text['result']['location']['lng']

    else:

        return None

    return lat, lng
print getlocation('武汉')
#链家网爬虫
house_info = []
p=86
df1 =pd.DataFrame(dict(diqu=[0], title=[0], town=[0], direction=[0], subway=[0], louceng=[0],  area=[0],fangxing=[0], price=[0]))
#链家网只有100页数据可读取，故下列通过循环读取100页数据并添加到house列表里
for page in range(87,101):
    web = 'https://wh.lianjia.com/zufang/pg%d/' %page

    response = requests.get(web,headers=hds[random.randint(0,len(hds)-1)])
    soup = BeautifulSoup(response.content, "lxml")
    price = [int(re.findall( '(\d+)',i.text)[0]) for i in soup.findAll(name='span',attrs={'class':'content__list--item-price'})]
    print(price)
    title=[]
    for k in soup.find_all('p', class_='content__list--item--title twoline'):  # ,找到div并且class为pl2的标签
        title1 = k.find_all('a')  # 在每个对应div标签下找span标签，会发现，一个a里面有四组span
        u=title1[0].string
        title.append(u)
    for j in range(len(title)):
        print(title[j])

    area1 = soup.find_all('p', class_= 'content__list--item--des')
    diqu=[]
    town=[]
    for j in area1:
        area2=j.find_all('a')
        if not area2:
            u = "no"
        else:
            u = area2[0].string
        diqu.append(u)
    for j in range(len(diqu)):
        print(diqu[j])
    for j in area1:
        area2=j.find_all('a')
        if not area2:
            u = "no"
        else:
            u = area2[1].string
        town.append(u)
    for j in range(len(town)):
        print(town[j])

    subway =[]
    direction=[]
    area=[]
    fangxing=[]
    louceng=[]
#朝向和城区数据在列表页没有，只有进入详细页读取，先要在列表页读取出详细页的网址,然后在详细页读取数据
    url_list = []

    list1 = soup.find_all('div', class_='content__list--item')
    for i in list1:
        url = i.find('a')['href']
        url_list.append(url)
    print(url_list)

    urls = re.findall('<a href="(.*?)" target="_blank">',str(soup.findAll(name='div',attrs = {'class':'pic-panel'})))
    for url in url_list:
        url = 'https://wh.lianjia.com'+url
        print(url)
        response1 = requests.get(url,headers= hds[random.randint(0,len(hds)-1)])
        soup1 = BeautifulSoup(response1.content,"lxml")
        l = soup1.find_all('div', class_='content__article__info')
        if not l:
            louceng.append("no")
        else:
            for k in l:
                l1 = k.find_all('li')
                u = l1[7].string
                louceng.append(u)
                print(u)
        m = soup1.find_all('p', class_='content__article__table')
        if not m:
            area.append("no")
            fangxing.append("no")
            direction.append("no")
        else:
            for k in m:
                m1 = k.find_all('span')
                area1 = m1[2].text
                fangxing1 = m1[3].text
                direction1 = m1[1].text
                area.append(area1)
                fangxing.append(fangxing1)
                direction.append(direction1)
        s = soup1.find_all('div', class_='content__article__info4')
        print(s)
        if not s:
            subway.append("no")
        else:
            for k in s:
                s1 = k.find_all('li')
                if not s1:
                    subway.append("no")
                else:
                    print(s1)
                    u = s1[0].text
                    subway.append(u)
                    print(u)
    df =pd.DataFrame(dict(diqu=diqu, title=title, town=town, direction=direction, subway=subway, louceng=louceng,  area=area,
             fangxing=fangxing, price=price))
    df1=pd.concat([df1,df],copy=False)
    print df1
    house_info.append(df)
    df1.to_csv("lianjia_wuhan5.csv",encoding="utf_8_sig",index=False)
    time.sleep(3)
    p +=1
    print p
