# -*- coding:utf-8 -*-  
import socket
from time import ctime,strftime,gmtime,localtime
import struct
import pymysql

host = '192.168.0.3' #监听所有的ip
port = 30000 #接口必须一致
bufsize = 48
addr = (host,port) 

udpServer = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
udpServer.bind(addr) #开始监听

def get_id():
    connect = pymysql.Connection(
        host='localhost',
        port=3306,
        user='utf8',
        passwd='sjzb@1024',
        db='utf8',
        charset='utf8')
    cursor = connect.cursor()
    sql = "SELECT * FROM ZigBee_111"
    id = cursor.execute(sql)
    cursor.close()
    return id + 1

def mysql_link():
    connect = pymysql.Connection(
        host='localhost',
        port=3306,
        user='utf8',
        passwd='sjzb@1024',
        db='utf8',
        charset='utf8')
    cursor = connect.cursor()
    connect.commit()
    cursor.close()
    return connect

def EditCode(data):
    head,netAddr,ftype,uuid,serial,reserve,stype,num,value = struct.unpack("1s4s1s32s1s1s2s2s4s",data)
    head = head.decode(encoding = 'utf-8')
    netAddr = netAddr.decode(encoding = 'utf-8')
    ftype = ftype.decode(encoding = 'utf-8')
    uuid = uuid.decode(encoding = 'utf-8')
    serial = serial.decode(encoding = 'utf-8')
    stype = stype.decode(encoding = 'utf-8')
    num = num.decode(encoding = 'utf-8')
    value = value.decode(encoding = 'utf-8')
    return head,netAddr,ftype,uuid,serial,stype,num,value

def EditSQL(id, netAddr , ftype , stype , num , value):
    now_time = strftime("%Y-%m-%d %H:%M:%S", localtime())
    sql = "INSERT INTO `ZigBee_111`(`id`, `netAddr`, `ftype`, `stype`, `num`, `value` ,`time`) VALUES ("
    sql = sql + str(id) + ",'"
    sql = sql + netAddr + "','"
    sql = sql + ftype + "','"
    sql = sql + stype + "','"
    sql = sql + num + "','"
    sql = sql + value + "','"
    sql = sql + now_time +"')"
    return sql

def backupSQL( id ):
    connect = pymysql.Connection(
        host='localhost',
        port=3306,
        user='utf8',
        passwd='sjzb@1024',
        db='utf8',
        charset='utf8')
    cursor = connect.cursor()
    try:
        sql = "DELETE FROM `ZigBee_111_History` WHERE 1"
        cursor.execute(sql)
    except Exception as e:
        print("backupSQL delete error:",e)
    try:
        sql = "insert into ZigBee_111_History select * from ZigBee_111"
        cursor.execute(sql)
    except Exception as e:
        print("backupSQL insert error:",e)
    try:
        sql = "truncate table ZigBee_111;"
        cursor.execute(sql)
        cursor.close()
        return 1
    except Exception as e:
        print("backupSQL truncate error:",e)
        return id

def Auth(head):
    if head=='&':
        return True
    else:
        return False

if __name__ == '__main__' :
    id = get_id()
    connect = mysql_link()
    webserver_udpserver_sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    while True:
        if id > 10000:
            id = backupSQL(id)
            
        try:
            data = udpServer.recv(bufsize) #接收数据和返回地址
            #print(data)
        except Exception as e:
            print("main udpServer.recv error:",e)
            continue
        #处理数据
        try:
            head , netAddr , ftype , uuid , serial , stype , num , value = EditCode( data )
        except Exception as e:
            print("main EditCode error:",e)
            continue     
        #验证头部是否符合要求
        if Auth(head)==False:
            continue
        #验证成功的数据发给webserver
        try:
            webserver_udpserver_sock.sendto(data,'/tmp/webserver_udpserver_socket.d')
        except Exception as e:
            print("main sendto error:",e)
        #插入到mysql
        try:
            sql = EditSQL( id , netAddr , ftype , stype , num , value )
            #print(sql)
            cursor = connect.cursor()
            cursor.execute(sql)
            connect.commit()
            cursor.close()
            id = id + 1
        except Exception as e:
            print("main EditSQL error:",e)
            
    connect.close()
    udpServer.close()
