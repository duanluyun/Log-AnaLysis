import MysqlUtils
def insetDayVidoAccessTopN(ls):
    db=MysqlUtils.getConnection()
    try:
        cursor=db.cursor()
        for i in ls:
            sql = "insert into day_video_access_topn_stat(day,cms_id,times) values ('%s','%d','d')"%(i.day,i.cmsId,i.times)
            cursor.execute(sql)
        db.commit()
    except:
        db.rollback()
    finally:
        db.close()


