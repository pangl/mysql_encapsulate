#encoding=utf-8

import configparser
#ConfigParser模块作用为读取写入配置文件
from utils.public_variables import config_path

class ConfigParse(object):
    def __init__(self):
        self.cf = configparser.ConfigParser()       #生成一个对象

    def get_db_conf(self):
        #获取db文件信息
        self.cf.read(config_path)                   #读取文件内容
        host = self.cf.get("mysqlconf", "host")     #获取db_config.ini的节点名称为mysqlconf的host信息
        port = self.cf.get("mysqlconf", "port")
        db = self.cf.get("mysqlconf", "db_name")
        user = self.cf.get("mysqlconf", "user")
        password = self.cf.get("mysqlconf", "password")
        return {"host":host,"port": int(port),"db":db,"user":user,"password":password}
        #返回一个字典对象

if __name__ == "__main__":
    cp = ConfigParse()
    print(cp.get_db_conf())

