import requests
import re
import pymongo
import time
from bs4 import BeautifulSoup

# 主要指标：http://f10.eastmoney.com/NewFinanceAnalysis/MainTargetAjax?type=0&code=SZ000001
# 资产负债表：http://f10.eastmoney.com/NewFinanceAnalysis/zcfzbAjax?companyType=4&reportDateType=0&reportType=1&endDate=&code=SZ000001
# 利润表：http://f10.eastmoney.com/NewFinanceAnalysis/lrbAjax?companyType=4&reportDateType=0&reportType=1&endDate=&code=SZ000001
# 现金流表：http://f10.eastmoney.com/NewFinanceAnalysis/xjllbAjax?companyType=4&reportDateType=0&reportType=1&endDate=&code=SZ000001
# 股权变动：http://dcfm.eastmoney.com/em_mutisvcexpandinterface/api/js/get?type=GBJG&token=70f12f2f4f091e459a279469fe49eca5&ps=3&st=CHANGEDATE&sr=-1&filter=(SECURITYCODE=%27000001%27)&js=var%20gbjgData=[(x)]

# SZ （000 002 300），SH （600 601 603）
client=pymongo.MongoClient("mongodb://mongoadmin:mongoadmin@47.100.220.26:27017")
db=client.stock
v=db.zh

columns = {0:"股票代码",1:"每股收益",2:"每股经营现金流",3:"净资产收益率",4:"总资产",5:"总负债",6:"总利润",7:"净利润",
           8:"经营现金流",9:"股本变动",10:"股权质押"}
code_begins = ["600","601","603","000","002","300"]
data_url = "http://data.eastmoney.com/stockdata/"
def get_data(stock_code):
    stock=[]
    stock.append(stock_code[2:])
    try:
        r_zyzb = requests.get("http://f10.eastmoney.com/NewFinanceAnalysis/MainTargetAjax?type=0&code="+stock_code,timeout = 3)
    except Exception as e:
        print('失败原因：%s' % e)
        return -1
    r_zyzb.json()
    r_zyzb_json = r_zyzb.json()
    if(len(r_zyzb_json)<=2):
        print(stock_code+":不存在该股票")
        return 0
    dict_jbmgsy = {}
    dict_mgjyxjl={}
    dict_jqjzcsyl={}
    for i in range(len(r_zyzb_json)):
        data = r_zyzb_json[i]['date']
        jbmgsy = r_zyzb_json[i]['jbmgsy']
        mgjyxjl = r_zyzb_json[i]['mgjyxjl']
        jqjzcsyl = r_zyzb_json[i]['jqjzcsyl']
        dict_jbmgsy[data] = jbmgsy
        dict_mgjyxjl[data] = mgjyxjl
        dict_jqjzcsyl[data] = jqjzcsyl
    stock.append(dict_jbmgsy)
    stock.append(dict_mgjyxjl)
    stock.append(dict_jqjzcsyl)
    try:
        r_zcfzb = requests.get("http://f10.eastmoney.com/NewFinanceAnalysis/zcfzbAjax?companyType=4&reportDateType=0&reportType=1&endDate=&code="+stock_code,timeout = 3)
    except Exception as e:
        print('失败原因：%s' % e)
        return -1
    r_zcfzb_text = r_zcfzb.text
    result1 = [str(item)[15:] for item in re.compile(r'REPORTDATE\\":\\"[^\\]*').findall(r_zcfzb_text)]
    result2 = [str(item)[13:] for item in re.compile(r'SUMASSET\\":\\"\d*').findall(r_zcfzb_text)]
    result3 = [str(item)[12:] for item in re.compile(r'SUMLIAB\\":\\"\d*').findall(r_zcfzb_text)]
    dict_zzc = {}
    dict_zfz = {}
    if(len(result1)!=0):
        for i in range(len(result1)):
            dict_zzc[result1[i]] = result2[i]
            dict_zfz[result1[i]] = result3[i]
    stock.append(dict_zzc)
    stock.append(dict_zfz)
    try:
        r_lrb = requests.get("http://f10.eastmoney.com/NewFinanceAnalysis/lrbAjax?companyType=4&reportDateType=0&reportType=1&endDate=&code="+stock_code,timeout = 3)
    except Exception as e:
        print('失败原因：%s' % e)
        return -1
    r_lrb_text = r_lrb.text
    result4 = [str(item)[15:] for item in re.compile(r'REPORTDATE\\":\\"[^\\]*').findall(r_lrb_text)]
    result5 = [str(item)[14:] for item in re.compile(r'SUMPROFIT\\":\\"\d*').findall(r_lrb_text)]
    result6 = [str(item)[14:] for item in re.compile(r'NETPROFIT\\":\\"\d*').findall(r_lrb_text)]
    dict_zlr = {}
    dict_jlr = {}
    if (len(result4) != 0):
        for i in range(len(result4)):
            dict_zlr[result4[i]] = result5[i]
            dict_jlr[result4[i]] = result6[i]
    stock.append(dict_zlr)
    stock.append(dict_jlr)
    try:
        r_xjllb = requests.get("http://f10.eastmoney.com/NewFinanceAnalysis/xjllbAjax?companyType=4&reportDateType=0&reportType=1&endDate=&code="+stock_code,timeout = 3)
    except Exception as e:
        print('失败原因：%s' % e)
        return -1
    r_xjllb_text = r_xjllb.text
    result7 = [str(item)[15:] for item in re.compile(r'REPORTDATE\\":\\"[^\\]*').findall(r_xjllb_text)]
    result8 = [str(item)[19:] for item in re.compile(r'NETINVCASHFLOW\\":\\"-{0,1}\d*').findall(r_xjllb_text)]
    dict_jyxjl = {}
    if(len(result7)!=0):
        for i in range(len(result7)):
            dict_jyxjl[result7[i]] = result8[i]
    stock.append(dict_jyxjl)
    try:
        r_gbbd = requests.get("http://f10.eastmoney.com/CapitalStockStructure/CapitalStockStructureAjax?code="+stock_code, timeout = 3)
    except Exception as e:
        print('失败原因：%s' % e)
        return -1
    r_gbbd_json = r_gbbd.json()
    dict_gbbd = {}
    if(len(r_gbbd_json['ShareChangeList'])!=0):
        dict_gbbd['data_list'] = r_gbbd_json['ShareChangeList'][0]['changeList'][:7]
        dict_gbbd['zgb_w_list'] = r_gbbd_json['ShareChangeList'][1]['changeList'][:7]
    else:
        dict_gbbd['data_list'] = []
        dict_gbbd['zgb_w_list'] = []
    stock.append(dict_gbbd)
    s = {}
    for i in range(len(stock)):
        s[columns[i]] = stock[i]
    v.insert_one(s)
    print(stock_code+"插入成功")
    return 0


success = 0
for i in range(0,len(code_begins)):
    num = 0
    code_begin = code_begins[i]
    while(num<1000):
        if(i<3):
            success = get_data("SH"+str(code_begin) + str(num).zfill(3))
        elif(i>=3&i<6):
            success = get_data("SZ"+str(code_begin) + str(num).zfill(3))
        else:
            print("该股不存在")
        if(success == 0):
            num = num + 1





