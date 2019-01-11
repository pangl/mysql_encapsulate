#coding=utf-8
#__author__=lilip

import pymysql
from utils.config_handler import ConfigParse

class DBOperation(object):
    def __init__(self):
        self.db_conf = ConfigParse().get_db_conf()  #获取db信息

    def db_connect(self,db_name=None):
        self.db_name = db_name
        try:
            conn = pymysql.connect(
                host=self.db_conf["host"],
                port=self.db_conf["port"],
                user=self.db_conf["user"],
                passwd=self.db_conf["password"],
                db = self.db_name,
                charset="utf8mb4")
            cur = conn.cursor()
            print("Mysql连接成功! ")
            return cur,conn
        except pymysql.Error as e:
            print("Mysql Error %d: %s" % (e.args[0], e.args[1]))

    def db_close(self):
        # 关闭数据连接
        self.conn.commit()
        self.cur.close()
        self.conn.close()

    def db_create(self,create_name):
        self.create_name = create_name
        try:
            self.cur, self.conn = self.db_connect()
            sql_statement_create = 'CREATE DATABASE IF NOT EXISTS ' + str(self.create_name) + \
                                   ' DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_0900_ai_ci;'
            self.cur.execute(sql_statement_create)
            self.db_close()
            print("创建数据库%s成功! "%(str(self.create_name)))
        except pymysql.Error as e:
            print("Mysql Error %d: %s" % (e.args[0], e.args[1]))

    def db_view(self):
        self.cur, self.conn = self.db_connect()
        self.cur.execute('show databases;')
        print('--------数据库查询结果如下--------')
        while 1:
            res = self.cur.fetchone()
            if res is None:
                break
            print(res[0])
        self.db_close()
        print('---------数据库查询完毕！---------')

    def db_delete(self,delete_name):
        self.delete_name = delete_name
        try:
            self.cur, self.conn = self.db_connect()
            sql_statement_delete = 'drop database %s;'%(self.delete_name)
            self.cur.execute(sql_statement_delete)
            print('删除成功!')
            self.cur.execute('show databases;')
            print('--------当前数据库信息如下--------')
            for db_item in self.cur.fetchall():
                print(db_item[0])
            print('--------------------------------')
            self.db_close()
        except pymysql.Error as e:
            print("Mysql Error %d: %s" % (e.args[0], e.args[1]))

if __name__ == '__main__':
    db=DBOperation()
    #db.db_connect()
    db.db_create('lilip1214')
    db.db_view()
    db.db_delete('lilip1214')
