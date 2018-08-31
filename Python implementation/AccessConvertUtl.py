from pyspark.sql.types import *
from pyspark.sql.types import StructType,StructField
from pyspark import Row
from IpUtils import getIp

fileds=[
    StructField("url",StringType()),
    StructField("cmsType",StringType()),
    StructField("cmsId", LongType()),
    StructField("traffic", LongType()),
    StructField("ip", StringType()),
    StructField("city", StringType()),
    StructField("time",StringType()),
    StructField("day",StringType())
]

def parselog(line):
    try:
        splits=line.split('\t')
        url=splits[1]
        traffic=int(splits[2])
        ip=splits[3]
        domain='http://www.imooc.com/'
        cms=url[url.index(domain)+len(domain):]
        cmsTypeId=cms.split('/')
        cmsType=''
        cmsId='ol'
        if len(cmsTypeId)>1:
            cmsType=cmsTypeId[0]
            cmsId=int(cmsTypeId[1])
        city=str(getIp(ip))
        time=splits[0]
        day=time[:10].replace('-','')
        return Row(url,cmsType,cmsId,traffic,ip,city,time,day)
    except:
        return Row(0)





