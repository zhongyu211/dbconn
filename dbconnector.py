# -*- coding: utf-8 -*-
# __author__ = 'mxins@qq.com'
import traceback

MYSQL = {
    'host': '127.0.0.1',
    'user': 'root',
    'passwd': '12359',
    'db': 'spark',
    'port': 3306
}
MYSQL = {
    'host': '127.0.0.1',
    'user': 'root',
    'passwd': 'shinezone244',
    'db': 'spark',
    'port': 3306
}

class DbService:
    def __init__(self, db='mysql'):
        self.db = db
        self.conn = None
        self.cursor = None

    def connect(self):
        if self.db == 'pg':
            import psycopg2
            self.conn = psycopg2.connect(**REDSHIFT)
            self.conn.autocommit = True
        else:
            import MySQLdb
            self.conn = MySQLdb.Connection(**MYSQL)
            self.conn.autocommit(True)
        self.cursor = self.conn.cursor()

    def close(self):
        self.cursor.close()
        self.conn.close()

    def execute(self, sql, args=None):
        self.connect()
        try:
            self.cursor.execute(sql, args)
        except Exception:
            raise Exception(traceback.format_exc())
        finally:
            self.close()

    def query(self, sql):
        self.connect()
        try:
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
        except Exception:
            raise Exception(traceback.format_exc())
        finally:
            self.close()
        return res

