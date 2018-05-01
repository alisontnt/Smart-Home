import os  
import socket  
import select  
import sys  
import threading
import time
import select
import queue
import pymysql

_host = 'localhost'
_post = 3306
_user = 'utf8'
_passwd = 'sjzb@1024'
_db = 'utf8'
_charset = 'utf8'

def work1():
    global conn
    while True:
        new_conn,new_addr=tcp_sock.accept()
        connect = pymysql.Connection(
            host=_host,
            port=_post,
            user=_user,
            passwd=_passwd,
            db=_db,
            charset=_charset)
        cursorsql = connect.cursor()
        sql_data = "DELETE FROM `ZigBee_table` WHERE 1"
        cursorsql.execute(sql_data)
        connect.commit()
        cursorsql.close()
        try:
            conn.close()
        except Exception as e:
            print("work1 error:",e)
            pass
        conn=new_conn
        addr=new_addr
        print(addr)


def work2():
    global conn
    while True:
        accept_data = str(p_sock.recv(48),encoding="utf8")
        try:
           conn.send(bytes(accept_data, encoding="utf8"))
        except Exception as e:
            print("work2 error:",e)
            pass
    conn.close()
    p_sock.close()

if __name__ == '__main__':
    accept_data=""
    p_sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)  
    path = '/tmp/tcpserver_webserver_socket.d'  
    if os.path.exists(path):  
        os.unlink(path)  
    p_sock.bind(path)  

    tcp_sock = socket.socket(socket.AF_INET , socket.SOCK_STREAM)
    tcp_sock.bind(('192.168.0.3',40000))
    tcp_sock.listen(5)

    thread1=threading.Thread(target=work1)
    thread2=threading.Thread(target=work2)
    thread1.start()
    thread2.start()



    
