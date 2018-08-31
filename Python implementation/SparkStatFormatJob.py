
#第一步清洗：抽取出我们所需的指定列
from pyspark.sql import SparkSession
from DateUtils import reformat

class SparkStatFormatJob:
    if __name__=='__main__':
        spark=SparkSession.builder.appName('SparkStatFormat')\
            .master('local[2]').getOrCreate()

        access=spark.sparkContext.textFile('file:///home/sam/Documents/spark/data/data2/10000_access.log')


        """
        splits=access.map(lambda line:line.split(' '))
        ip=splits.map(lambda x:x[0])
        time=splits.map(lambda x:x[3]+''+x[4])
        url=splits.map(lambda x:x[11])
        traffic=splits.map(lambda x:x[9])
        """
        """
        原始日志ip和日期的格式
        ('183.162.52.7', '[10/Nov/2016:00:01:02 +0800]')==>YYYY/MM/DD  HH:mm:ss
       """

        result=access.map(lambda line:line.split(' '))\
            .map(lambda x:(x[0],reformat(x[3]+' '+x[4]),x[11].replace('\"',""),x[9]))\
            .map(lambda x:(x[1]+'\t'+x[2]+'\t'+x[3]+'\t'+x[0])).saveAsTextFile("file:///home/sam/Documents/spark/data/logoutput")

        spark.stop()




