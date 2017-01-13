# -*- coding: utf-8 -*-
# __author__ = 'allen zhong'
import psycopg2
import traceback
import time
class pgconn():
    def __init__(self,work_content,dbname='****',user='***',password='******', host='****************',port=5439):
        self.database = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.sql = work_content
        self.retrys = 10

    def dowork(self):
        try:
            conn = psycopg2.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database)
            conn.autocommit =True
            cur = conn.cursor()
            cur.execute(self.sql)
            cur.close()
            conn.close()
            print "upload successfully."
        except Exception as ex:
            #InternalError fired when excute sql failed, OperationalError fired when connection failed
            if ex.__class__.__name__ in ["InternalError","OperationalError"] and self.retrys>0:
                self.retrys -= 1
                time.sleep(5)
                print "Exception: %s, Retry: %d times."%(ex.__class__.__name__,10-self.retrys)
                self.dowork()
            else:
                traceback.print_exc()

    def query(self):
        try:
            conn = psycopg2.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database)
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(self.sql)
            res = cur.fetchall()
            cur.close()
            conn.close()
            return res
            print "upload successfully."
        except Exception as ex:
            if ex.__class__.__name__ in ["InternalError", "OperationalError"] and self.retrys > 0:
                self.retrys -= 1
                time.sleep(5)
                print "Exception: %s, Retry: %d times." % (ex.__class__.__name__, 10 - self.retrys)
                self.query()
            else:
                traceback.print_exc()
