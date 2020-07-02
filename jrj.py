# -*- coding: utf-8 -*-
import pymongo
from bs4 import BeautifulSoup
import requests

result=0
client=pymongo.MongoClient("mongodb://mongoadmin:mongoadmin@47.100.220.26:27017")
db=client.stock
v=db.wy
print(v)
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36"}

def run(apiUrl, code):
    diff=code[0:3]
    plate=""
    if diff=="600" or diff=="601" or diff=="603" or diff=="000":
        plate="主板"
    elif diff=="002":
        plate="中小板"
    elif diff=="300":
        plate="创业板"
    elif diff=="688":
        plate="科创板"

    global result
    sy=requests.get(apiUrl,headers=headers)
    gsgk=requests.get("http://stock.jrj.com.cn/share,"+code+",gsgk.shtml",headers=headers)
    sdgd=requests.get("http://stock.jrj.com.cn/share," + code + ",sdgd.shtml", headers=headers)
    ggry=requests.get("http://stock.jrj.com.cn/share,"+code+",ggry.shtml",headers=headers)
    jgpj = requests.get("http://stock.jrj.com.cn/share," + code + ",jgpj.shtml", headers=headers)
    gbjg = requests.get("http://stock.jrj.com.cn/share," + code + ",gbjg.shtml", headers=headers)
    lrfpb=requests.get("http://stock.jrj.com.cn/share," + code + ",lrfpb.shtml", headers=headers)

    sdgdtxt = getSoup(sdgd)
    gbjgtxt = getSoup(gbjg)
    ggrytxt = getSoup(ggry)
    sytxt = getSoup(sy)
    gsgktxt = getSoup(gsgk)
    jgpjtxt = getSoup(jgpj)
    lrfpbtxt = getSoup(lrfpb)

    if sy.status_code==404 or len(sdgdtxt.find_all('table', class_='tab1'))==0 or len(gbjgtxt.find_all('table',class_='str'))==0 \
            or len(ggrytxt.find_all('div',class_='tabs'))==0 or len(lrfpbtxt.find_all('table',class_='tab1'))==0:
        return 0
    elif sy.status_code==200:
        if len(sdgdtxt.find_all('table', class_='tab1')[0].find_all('tr'))==2:
            tui=sytxt.find_all('span',id='quote_name')[0].text
            print(tui)
            print('已退市')
            try:
                s = {
                    '公司全称': tui,
                    '股票代码': code,
                    '板块':plate,
                    '状态':'已退市',
                }
                v.insert_one(s)
                result += 1
            except:
                pass

        else:
            name = gsgktxt.find_all('div', class_='tabs')[0].find_all('td')[0].text
            main_bussiness = gsgktxt.find_all('div', class_='tabs')[0].find_all('td')[-1].text
            registered_capital = gsgktxt.find_all('div', class_='tabs')[0].find_all('td')[5].text
            real_control=ggrytxt.find_all('div',class_='tabs')[0].find_all('td')[0].text
            issue_price=str(sytxt.find_all('div',class_='gg_box')[0].find_all('p')[1].contents)[16:-3]
            total_equity=gbjgtxt.find_all('table',class_='str')[0].find_all('td')[1].text
            flow_equity=gbjgtxt.find_all('table',class_='str')[0].find_all('td')[3].text
            total_income=str(lrfpbtxt.find_all('table',class_='tab1')[0].find_all('td')[11].text)[0:-2]
            total_benefit=str(lrfpbtxt.find_all('table',class_='tab1')[0].find_all('tr')[22].find_all('td')[1].text)[0:-2]
            senior_executive=[]
            for i in range(len(ggrytxt.find_all('table',class_='tab1')[2].find_all('tr'))-1):
                senior_executive.append(ggrytxt.find_all('table',class_='tab1')[2].find_all('tr')[i+1].find_all('td')[0].text)
            level_item={
                '综合评级':jgpjtxt.find_all('td',class_='tc tds')[1].text,
                '市盈率(TTM)':jgpjtxt.find_all('table',class_='tab1 tab2')[2].find_all('td')[4].text,
                '市净率(MRQ)': jgpjtxt.find_all('table', class_='tab1 tab2')[2].find_all('td')[8].text,
                '市现率(TTM)': jgpjtxt.find_all('table', class_='tab1 tab2')[2].find_all('td')[12].text,
            }
            type_item={
                '证监会行业分类':gsgktxt.find_all('div', class_='tabs')[0].find_all('td')[7].text,
                '全球行业分类':gsgktxt.find_all('div', class_='tabs')[0].find_all('td')[8].text,
                '申万行业分类':gsgktxt.find_all('div', class_='tabs')[0].find_all('td')[9].text,
            }
            shareholderlist=[]
            for i in range(len(sdgdtxt.find_all('table', class_='tab1')[0].find_all('tr'))-1):
                shareholder = []
                shareholder_item = {
                    '股东名称':sdgdtxt.find_all('table', class_='tab1')[0].find_all('tr')[i+1].find_all('td')[1].text,
                    '持股数':sdgdtxt.find_all('table', class_='tab1')[0].find_all('tr')[i+1].find_all('td')[2].text,
                    '占总股本比例':sdgdtxt.find_all('table', class_='tab1')[0].find_all('tr')[i+1].find_all('td')[3].text,
                }
                shareholderlist.append(shareholder_item)

            print(name)
            print(main_bussiness)
            print(registered_capital)
            print(real_control)
            print(issue_price)

            try:
                s = {
                    '公司全称': name,
                    '股票代码':code,
                    '板块': plate,
                    '发行价格（元）':issue_price,
                    '主营业务': main_bussiness,
                    '注册资本（元）': registered_capital,
                    '实际控制人':real_control,
                    '股东列表':shareholderlist,
                    '行业分类':type_item,
                    '总股本(万股)':total_equity,
                    '总流通股本(万股)':flow_equity,
                    '主营收入(万元)':total_income,
                    '净利润(万元)':total_benefit,
                    '高管列表':senior_executive,
                    '评级':level_item,
                    '状态': '上市',
                    '数据源': '金融界',
                }
                v.insert_one(s)
                result += 1
            except:
                pass

def getCode(i):
    if i >= 1 and i <= 999:
        return str(i).zfill(6)
    elif i >= 1000 and i <= 1998:
        return "002" + str(i - 999).zfill(3)
    elif i >= 1999 and i <= 2997:
        return "300" + str(i - 1998).zfill(3)
    elif i >= 2998 and i <= 3996:
        return "600" + str(i - 2998).zfill(3)
    elif i >= 3997 and i <= 4995:
        return "601" + str(i - 3997).zfill(3)
    elif i >= 4996 and i <= 5994:
        return "603" + str(i - 4996).zfill(3)
    else:
        return "688" + str(i - 5995).zfill(3)

def getSoup(url):
    url.encoding = "GBK"
    html = url.text
    soup = BeautifulSoup(html, 'html.parser')
    return soup

if __name__ == "__main__":
    print("启动爬虫，开始爬取数据")
    for i in range(6047,7000):
        code=getCode(i)
        run("http://stock.jrj.com.cn/share,"+code+".shtml",code)
    print("爬虫结束，共为您爬取到 {} 条数据".format(result))

