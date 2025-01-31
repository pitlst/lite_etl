# 手动指定模块的导入路径
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

def main():
    from utils import CONNECTER, CONFIG
    print("连接数据库测试")
    for ch in CONFIG.CONNECT_CONFIG:
        if CONFIG.CONNECT_CONFIG[ch]["type"] != "mongo":
            connect = CONNECTER[ch].connect()
            connect.close()
        else:
            client = CONNECTER[ch]
            print("mongo的所有数据有" + str(client.list_database_names()))
        print("连接 " + ch + " 成功")

if __name__ == "__main__":
    main()

