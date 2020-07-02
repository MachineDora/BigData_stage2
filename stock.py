
from  concurrent import futures
import pymongo
from bs4 import BeautifulSoup
import requests


result=0
client=pymongo.MongoClient("mongodb://mongoadmin:mongoadmin@47.100.220.26:27017")
db=client.stock
v=db.crz

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36"}

def getCode(i):
    if i>=1 and i<=999:
        return str(i).zfill(6)
    elif i>=1000 and i<=1998:
        return "002"+str(i-999).zfill(3)
    elif i>=1999 and i<=2997:
        return "300"+str(i-1998).zfill(3)
    elif i>=2998 and i<=3996 :
        return "600"+str(i-2997).zfill(3)
    elif i>=3997 and i<=4995:
        return "601"+str(i-3996).zfill(3)
    elif i>=4996 and i<=5994:
        return "603"+str(i-4995).zfill(3)
    else :
        return "688"+str(i-5994).zfill(3)

def run(apiUrl,code):
    print(apiUrl)

    gudongList=[]#控股列表
    managerList=[]#高管列表
    global result
    req=requests.get(apiUrl,headers=headers)#源网页
    if req.status_code==404:
        return 0
    elif req.status_code==200:
        req.encoding = "utf-8"
        req1 = requests.get("http://company.cnstock.com/f10/gsjj/sz{}.html".format(code), headers=headers)  # 公司信息
        req2 = requests.get("http://company.cnstock.com/f10/gdqk/sz{}.html".format(code), headers=headers)  # 股东信息
        req3 = requests.get("http://company.cnstock.com/f10/gbqk/sz{}.html".format(code), headers=headers)  # 股本信息
        req4 = requests.get("http://company.cnstock.com/f10/cbzy/sz{}.html".format(code), headers=headers)  # 财报信息
        req5 = requests.get("http://company.cnstock.com/f10/glc/sz{}.html".format(code), headers=headers)  # 高管
        html = req.text
        html1=req1.text
        html2=req2.text
        html3=req3.text
        html4=req4.text
        html5=req5.text
        soup = BeautifulSoup(html, 'html.parser')
        soup1 = BeautifulSoup(html1, 'html.parser')
        soup2=BeautifulSoup(html2,'html.parser')
        soup3 = BeautifulSoup(html3, 'html.parser')
        soup4 = BeautifulSoup(html4, 'html.parser')
        soup5=BeautifulSoup(html5,'html.parser')

        try:
            name = soup.find_all('span', class_='name mr5')[0].text  # 公司简称
            nameAll = soup1.find_all('table', class_='tablef_info')[0].find_all('td')[0].text  # 公司全称
            beginTime = soup1.find_all('table', class_='tablef_info')[0].find_all('td')[1].text  # 上市日期
            rigiMoney = soup1.find_all('table', class_='tablef_info')[0].find_all('td')[4].text  # 注册资本
            todayBegin = soup.find('ul', class_='fn-clear').find_all('li')[2].text  # 今日开盘
            yesterEnd = soup.find('ul', class_='fn-clear').find_all('li')[3].text  # 昨日收盘
            GuDong1 = soup2.find_all('table', class_='table_data')[1].find('strong').text  # 控股股东
            gudongAll = soup2.find_all('table', class_='table_data')[1].find_all('strong')  # 控股股东
            gudongData = soup2.find_all('table', class_='table_data')[1].find('tbody')
            AllGB = soup3.find('table').find_all('tr')[0].find('td').text  # 总股本
            AllrowGB = soup3.find('table').find_all('tr')[1].find('td').text  # 总流通股本
            moneyPerG = soup4.find('tbody').find_all('tr')[1].find('td').text  # 每股净资产
            allIncome = soup4.find('tbody').find_all('tr')[5].find('td').text  # 主营收入
            allProfit = soup4.find('tbody').find_all('tr')[6].find('td').text  # 净利润
            allManager10 = soup5.find('tbody').find_all('strong')
            for i in range(len(gudongAll)):
                item = {
                    '股东名称': gudongAll[i].text,
                    '持股数': gudongData.find_all('tr')[i].find_all('td')[1].text,
                    '占总股本比例': gudongData.find_all('tr')[i].find_all('td')[2].text,
                }
                gudongList.append(item)
            length = 0
            if len(allManager10) >= 10:
                length = 10
            else:
                length = len(allManager10)
            for j in range(length):
                managerList.append(allManager10[j].text)
            s = {
                '股票代码': code,
                '公司名称':name,
                '上市时间':beginTime,
                '公司全称':nameAll,
                '注册资本(万元)':rigiMoney,
                '股东列表':gudongList,
                '总股本(万股)':AllGB,
                '总流通股本(万股)':AllrowGB,
                '每股净资产(元/股)':moneyPerG,
                '主营收入(万元)':allIncome,
                '净利润(万元)':allProfit,
                '高管列表':managerList,
                '数据源':"中国证券网"
            }
            v.insert_one(s)
            result+=1
        except:
            pass

if __name__ == "__main__":
    print("启动爬虫，开始爬取数据")
    for i in range(1430, 7000):
        code=getCode(i)
        apiUrl="http://stockdata.cnstock.com/stock/sz{}.html".format(code)
        run(apiUrl,code)
    print("爬虫结束，共为您爬取到 {} 条数据".format(result))