import pymysql

def getConnection():
    return pymysql.connect('localhost','root','dly920329','imooc_project')

def close(db):
    db.close()





