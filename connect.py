import pymongo

result=0
client=pymongo.MongoClient("mongodb://mongoadmin:mongoadmin@47.100.220.26:27017")
db=client.stock
crz=db.crz
wy=db.wy
zh=db.zh
all=db.all

def getCode(i):
    if i>=1 and i<=999:
        return str(i).zfill(6)
    elif i>=1000 and i<=1998:
        return "002"+str(i-999).zfill(3)
    elif i>=1999 and i<=2997:
        return "300"+str(i-1998).zfill(3)
    elif i>=2998 and i<=3997 :
        return "600"+str(i-2998).zfill(3)
    elif i>=3998 and i<=4997:
        return "601"+str(i-3998).zfill(3)
    elif i>=4998 and i<=5997:
        return "603"+str(i-4998).zfill(3)
    else :
        return "688"+str(i-5998).zfill(3)

if __name__ == "__main__":
    print("开始整合数据")
    result=0
    for i in range(3155, 7000):
        code=getCode(i)
        data_wy = wy.find({'股票代码': str(code)})
        data_crz = crz.find({'股票代码': str(code)})
        data_zh = zh.find({'股票代码': str(code)})
        length_wy=len(list(data_wy))
        length_crz=len(list(data_crz))
        length_zh=len(list(data_zh))
        if length_wy==1 and length_crz==1 and length_zh==1:#三个数据源都有数据
            data_wy = wy.find({'股票代码': str(code)})
            data_crz=crz.find({'股票代码': str(code)})
            data_zh=zh.find({'股票代码': str(code)})
            list_wy=list(data_wy)[0]
            list_crz=list(data_crz)[0]
            list_zh=list(data_zh)[0]
            if list_wy['状态']=='已退市':
                s={
                    '股票代码':str(code),
                    '公司简称':list_crz['公司名称'],
                    '公司全称':list_crz['公司全称'],
                    '上市时间':list_crz['上市时间'],
                    '注册资本(万元)':list_crz['注册资本(万元)'],
                    '实际控制人': '--',
                    '股东详情':list_crz['股东列表'],
                    '总股本(万股)':list_crz['总股本(万股)'],
                    '总流通股本(万股)':list_crz['总流通股本(万股)'],
                    '每股净资产(元/股)':list_crz['每股净资产(元/股)'],
                    '主营收入(万元)': list_crz['主营收入(万元)'],
                    '净利润(万元)': list_crz['净利润(万元)'],
                    '高管列表': list_crz['高管列表'],
                    '板块':list_wy['板块'],
                    '发行价格': '--',
                    '主营业务': '--',
                    '行业分类': '--',
                    '评级': '--',
                    '状态':list_wy['状态'],
                    '每股收益': list_zh['每股收益'],
                    '每股经营现金流': list_zh['每股经营现金流'],
                    '净资产收益率': list_zh['净资产收益率'],
                    '总资产': list_zh['总资产'],
                    '总负债': list_zh['总负债'],
                    '总利润': list_zh['总利润'],
                    '净利润': list_zh['净利润'],
                    '经营现金流': list_zh['经营现金流'],
                    '股本变动': list_zh['股本变动'],
                    '数据源':'中国证券网&东方财富网'
                }
                all.insert_one(s)
                result+=1
                print(result)
            else:
                s={
                    '股票代码':str(code),
                    '公司简称':list_crz['公司名称'],
                    '公司全称':list_crz['公司全称'],
                    '上市时间':list_crz['上市时间'],
                    '注册资本(万元)':list_crz['注册资本(万元)'],
                    '实际控制人': list_wy['实际控制人'],
                    '股东详情':list_crz['股东列表'],
                    '总股本(万股)':list_crz['总股本(万股)'],
                    '总流通股本(万股)':list_crz['总流通股本(万股)'],
                    '每股净资产(元/股)':list_crz['每股净资产(元/股)'],
                    '主营收入(万元)': list_crz['主营收入(万元)'],
                    '净利润(万元)': list_crz['净利润(万元)'],
                    '高管列表': list_crz['高管列表'],
                    '板块':list_wy['板块'],
                    '发行价格': list_wy['发行价格（元）'],
                    '主营业务': list_wy['主营业务'],
                    '行业分类': list_wy['行业分类'],
                    '评级': list_wy['评级'],
                    '状态':list_wy['状态'],
                    '每股收益': list_zh['每股收益'],
                    '每股经营现金流': list_zh['每股经营现金流'],
                    '净资产收益率': list_zh['净资产收益率'],
                    '总资产': list_zh['总资产'],
                    '总负债': list_zh['总负债'],
                    '总利润': list_zh['总利润'],
                    '净利润': list_zh['净利润'],
                    '经营现金流': list_zh['经营现金流'],
                    '股本变动': list_zh['股本变动'],
                    '数据源':'中国证券网&金融界&东方财富网'
                }
                all.insert_one(s)
                result+=1
                print(result)
        if i>=2998 and length_crz==0 and length_wy==1 and length_zh==1:
            data_wy = wy.find({'股票代码': str(code)})
            data_zh = zh.find({'股票代码': str(code)})
            list_wy = list(data_wy)[0]
            list_zh = list(data_zh)[0]
            if list_wy['状态']=='已退市':
                s={
                    '股票代码':str(code),
                    '公司简称':'--',
                    '公司全称':list_wy['公司全称'],
                    '上市时间':'--',
                    '注册资本(万元)':'--',
                    '实际控制人': '--',
                    '股东详情':'--',
                    '总股本(万股)':'--',
                    '总流通股本(万股)':'--',
                    '每股净资产(元/股)':'--',
                    '主营收入(万元)': '--',
                    '净利润(万元)': '--',
                    '高管列表': '--',
                    '板块':list_wy['板块'],
                    '发行价格': '--',
                    '主营业务': '--',
                    '行业分类': '--',
                    '评级': '--',
                    '状态':list_wy['状态'],
                    '每股收益': list_zh['每股收益'],
                    '每股经营现金流': list_zh['每股经营现金流'],
                    '净资产收益率': list_zh['净资产收益率'],
                    '总资产': list_zh['总资产'],
                    '总负债': list_zh['总负债'],
                    '总利润': list_zh['总利润'],
                    '净利润': list_zh['净利润'],
                    '经营现金流': list_zh['经营现金流'],
                    '股本变动': list_zh['股本变动'],
                    '数据源':'金融界&东方财富网'
                }
                all.insert_one(s)
                result+=1
                print(result)
            else:
                s={
                    '股票代码':str(code),
                    '公司简称':'--',
                    '公司全称':list_wy['公司全称'],
                    '上市时间':'--',
                    '注册资本(万元)':float(str(list_wy['注册资本（元）']).replace(',',''))/10000,
                    '实际控制人': list_wy['实际控制人'],
                    '股东详情':list_wy['股东列表'],
                    '总股本(万股)':list_wy['总股本(万股)'],
                    '总流通股本(万股)':list_wy['总流通股本(万股)'],
                    '每股净资产(元/股)':'--',
                    '主营收入(万元)': list_wy['主营收入(万元)'],
                    '净利润(万元)': list_wy['净利润(万元)'],
                    '高管列表': list_wy['高管列表'],
                    '板块':list_wy['板块'],
                    '发行价格': list_wy['发行价格（元）'],
                    '主营业务': list_wy['主营业务'],
                    '行业分类': list_wy['行业分类'],
                    '评级': list_wy['评级'],
                    '状态':list_wy['状态'],
                    '每股收益': list_zh['每股收益'],
                    '每股经营现金流': list_zh['每股经营现金流'],
                    '净资产收益率': list_zh['净资产收益率'],
                    '总资产': list_zh['总资产'],
                    '总负债': list_zh['总负债'],
                    '总利润': list_zh['总利润'],
                    '净利润': list_zh['净利润'],
                    '经营现金流': list_zh['经营现金流'],
                    '股本变动': list_zh['股本变动'],
                    '数据源':'金融界&东方财富网'
                }
                all.insert_one(s)
                result += 1
                print(result)
    print("整合结束，共为您整合到 {} 条数据".format(result))