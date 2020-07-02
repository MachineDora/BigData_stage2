import pymongo
import os
import csv
import time

result=0
client=pymongo.MongoClient("mongodb://mongoadmin:mongoadmin@47.100.220.26:27017")
db=client.stock
year=db.hks

def eachFile(filepath):
    pathDir =  os.listdir(filepath)
    for allDir in pathDir:
        child = os.path.join('%s\\%s' % (filepath, allDir))
        with open(child,'r',encoding='UTF-8') as f:
            reader=csv.reader(f)
            count=0
            for row in reader:
                if count==0:
                    count+=1
                    continue
                count+=1
                print(row)
                s={
                    '股票代码':str(row[1]).split('.')[0],
                    '交易日期':str(row[2])[0:4]+"-"+str(row[2])[4:6]+"-"+str(row[2])[6:8],
                    '开盘价':str(row[3]),
                    '最高价': str(row[4]),
                    '最低价': str(row[5]),
                    '收盘价': str(row[6]),
                    '昨日收盘价':str(row[7]),
                    '涨跌额': str(row[8]),
                    '成交量(手)': str(row[10]),
                    '成交额(千元)': str(row[11]),
                }
                year.insert_one(s)

if __name__ == '__main__':

    filePathC = "D:\\NJU\\大三\\下\\大数据集成\\一年的数据\\daily_data"
    eachFile(filePathC)
