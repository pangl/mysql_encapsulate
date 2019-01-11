#encoding=utf-8
import os

#工程的根目录
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

#数据库配置文件绝对路径
config_path = BASE_DIR + "/config/db_config.ini"