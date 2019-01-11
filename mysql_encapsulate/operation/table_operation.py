#coding=utf-8
import pymysql
import sys
from utils.config_handler import ConfigParse

class TableOperation(object):
    def __init__(self):
        self.db_conf = ConfigParse().get_db_conf()  #获取db信息

    def connect(self,name):
        self.name = name
        try:
            conn = pymysql.connect(
                host=self.db_conf["host"],
                port=self.db_conf["port"],
                user=self.db_conf["user"],
                passwd=self.db_conf["password"],
                db = self.name,
                charset="utf8mb4")
            cur = conn.cursor()
            print("%s连接成功! "%(self.name))
            return cur,conn
        except pymysql.Error as e:
            print("Mysql Error %d: %s" % (e.args[0], e.args[1]))

    def db_close(self):
        # 关闭数据连接
        self.conn.commit()
        self.cur.close()
        self.conn.close()

    def table_create(self,db_name,table_name,column_info):
        #column_info分别传入列名，列类型，类型范围
        try:
            self.cur, self.conn = self.connect(db_name)
            self.table_name = table_name
            # 如果所建表已存在，删除重建
            sql_statement_exists = "drop table if exists %s;"%(self.table_name.lower())
            self.cur.execute(sql_statement_exists)
            # 执行建表sql语句
            sql_statement_create = "CREATE TABLE `%s`(" % (self.table_name)
            for column_item in column_info:
                if len(column_item)==3:
                    sql_statement_create += '''`%s` %s(%s) DEFAULT NULL,'''%(
                        str(column_item[0]),str(column_item[1]),str(column_item[2]))
                elif len(column_item)==2:
                    sql_statement_create += '''`%s` %s DEFAULT NULL,''' % (
                        str(column_item[0]), str(column_item[1]))
                elif len(column_item)==0:
                    print('第%s列中无内容，请重新输入！'%(str(column_info.index(column_item)+1)))
                    return
                else:
                    print('%s列格式有误，请重新输入！'%(column_item))
                    return
            sql_statement_create=sql_statement_create[:-1]
            sql_statement_create += ')ENGINE=innodb DEFAULT CHARSET=utf8mb4;'
            self.cur.execute(sql_statement_create)
            self.db_close()
            print("创建数据表成功!")
        except pymysql.Error as e:
            print("Mysql Error %d: %s" % (e.args[0], e.args[1]))

    def table_view(self,db_name,table_name,view_num='all'):
        try:
            self.cur, self.conn = self.connect(db_name)
            sql_statement_view = "select * from %s;"%(table_name.lower())
            self.cur.execute(sql_statement_view)
            if isinstance(view_num,int):
                length = len(self.cur.fetchall())
                if 0 < view_num <= length:
                    self.cur.execute(sql_statement_view)
                    resTuple = self.cur.fetchmany(view_num)
                    print('--------%s表查询的%s条数据如下--------' % (table_name,view_num))
                    for i in resTuple:
                        print(i)
                    print('----------%s表查询完毕！----------' % (table_name))
                elif view_num==0:
                    print('查询的数目请大于0！')
                else:
                    print('查询的数目大于数据库的最大内容条数!\n将为您打印全部内容~')
                    self.cur.execute(sql_statement_view)
                    for i in self.cur.fetchall():
                        print(i)
                    print('----------%s表查询完毕！----------' % (table_name))
            elif view_num.lower()=='all':
                self.cur.execute(sql_statement_view)
                print('--------%s表查询的全部结果如下--------' % (table_name))
                while 1:
                    res = self.cur.fetchone()
                    if res is None:
                        break
                    print(res)
                print('----------%s表查询完毕！----------'%(table_name))
            else:
                print('查询数目格式必须是正整数，请重新输入整数！')
            self.db_close()
        except pymysql.Error as e:
            print("Mysql Error %d: %s" % (e.args[0], e.args[1]))

    def table_insert(self,db_name,table_name,insert_content):
        try:
            self.cur,self.conn = self.connect(db_name)
            sql_statement_count_column="select * from information_schema.columns " \
                                       "where table_schema='%s' and table_name='%s'"%(db_name,table_name)
            self.cur.execute(sql_statement_count_column)
            length = self.cur.execute(sql_statement_count_column)
            if isinstance(insert_content,tuple):
                if length == len(insert_content):
                    sql_statement_insert = "insert into %s values("%(table_name)
                    for i in range(length):
                        sql_statement_insert += '%s,'
                    sql_statement_insert=sql_statement_insert[:-1]+')'
                    self.cur.execute(sql_statement_insert,insert_content)
                    print('数据已成功插入%s表！'%(table_name))
                else:
                    print('插入的数据个数与表的列数不匹配，请重新输入！')
            else:
                print('插入的内容类型应该为元组，请重新输入！')
            self.db_close()
        except pymysql.Error as e:
            print("Mysql Error %d: %s" % (e.args[0], e.args[1]))

    def table_update(self,db_name,tb_name,update_statement,update_condition=None):
        #db_name:数据库名称，tb_name：表名,update_statement：更新内容sql语句,update_condition：更新条件sql语句
        try:
            self.cur, self.conn = self.connect(db_name)
            sql_statement_update='update %s set %s'%(tb_name,update_statement)
            if update_condition:
                sql_statement_update += ' where %s'%(update_condition)
            self.cur.execute(sql_statement_update)
            print('内容修改成功！最新结果为：')
            sql_statement_view='select * from %s'%(tb_name)
            if update_condition:
                sql_statement_view += ' where %s;'%(update_condition)
            else:
                sql_statement_view +=';'
            self.cur.execute(sql_statement_view)
            for i in self.cur.fetchall():
                print(i)
            self.db_close()

        except pymysql.Error as e:
            print("Mysql Error %d: %s" % (e.args[0], e.args[1]))


if __name__ == '__main__':
    t=TableOperation()
    #t.connect()
    #t.table_create('lilip','table_create1214',[('id','int'),('name','varchar',255),
                    #('password','varchar',255),('date','date')])
    #t.table_view('lilip','table_create1214')
    #t.table_insert('lilip','table_create1214',(2,'python','','2017-12-44'))
    #t.table_update('lilip','user',['birthday'],[('hello','1999-9-4'),('world','1985-3-2')],
                   #"where name='hello' or name='world'")
    t.table_update('lilip','user',"password='mima',id='1217'","name = 'test' or birthday = '1999-09-04'")
