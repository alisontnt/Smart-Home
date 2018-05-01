# -*- coding:utf-8 -*-
import json
import pymysql
import socket
import sys
import os 
from flask import Flask,request,abort
import multiprocessing  
import struct

_host = 'localhost'
_post = 3306
_user = 'utf8'
_passwd = 'sjzb@1024'
_db = 'utf8'
_charset = 'utf8'

uuid_num = 10

app = Flask(__name__)

#非法请求返回404状态码
@app.route('/')
def my_abort():
    abort(404)

#signin用于客户端请求认证登录信息
@app.route('/signin',methods=['post'])
def signin():
    try:
        receive_data=request.json
        tag = receive_data['TAG']
    except:
        abort(404)
    if(tag!='signin'):
        abort(404)
    try:
        if (auth(receive_data['USERNAME'],receive_data['PASSWORD'])==True):
            send_data={'TAG':receive_data['TAG'],'AUTH':True}
        else:
            send_data={'TAG':receive_data['TAG'],'AUTH':False}
    except Exception as e:
        print("signin error:",e)
        abort(500)
    return json.dumps(send_data)

#data用于客户端请求数据，返回最近上传的数据
@app.route('/data',methods=['post'])
def data():
    try:
        receive_data=request.json
        tag=receive_data['TAG']
    except:
        abort(404)
    if (tag != 'data'):
        abort(404)
    try:
        if (auth(receive_data['USERNAME'],receive_data['PASSWORD'])==True):
            send_data={'TAG':receive_data['TAG'],'AUTH':True,'DATA':get_data()}
        else:
            send_data={'TAG':receive_data['TAG'],'AUTH':False}
    except Exception as e:
        print("data error:",e)
        abort(500)
    return json.dumps(send_data)

#data/类型(stype，两个字节)序号(num，两个字节)用于客户端请求指定传感器的数据，返回该传感器最近上传的数据，如果传感器不存在则
@app.route('/data/<mytype>',methods=['post'])
def designated_data(mytype):
    try:
        receive_data=request.json
        tag=receive_data['TAG']
    except:
        abort(404)
    if (tag != 'data'):
        abort(404)
    try:
        if (auth(receive_data['USERNAME'],receive_data['PASSWORD'])==True):
            send_data={'TAG':receive_data['TAG'],'AUTH':True,'DATA':get_designated_data(mytype[0:2],mytype[2:4])}
        else:
            send_data={'TAG':receive_data['TAG'],'AUTH':False}
    except Exception as e:
        print("data<mytype> error:",e)
        abort(500)
    return json.dumps(send_data)

#cmd用于客户端请求发送命令到传感器
@app.route('/cmd',methods=['post'])
def cmd():
    try:
        receive_data=request.json
        tag = receive_data['TAG']
    except:
        abort(404)
    if (tag != 'cmd'):
        abort(404)
    try:
        if (auth(receive_data['USERNAME'],receive_data['PASSWORD'])==True):
            send_data={'TAG':receive_data['TAG'],'AUTH':True,'DATA':send_cmd(receive_data['CMD'])}
        else:
            send_data={'TAG':receive_data['TAG'],'AUTH':False}
    except Exception as e:
        print("cmd error:",e)
        abort(500)
    return json.dumps(send_data)

#cmdback用于客户端请求命令回传，即传感器命令执行后
@app.route('/cmdback',methods=['post'])
def cmdback():
    try:
        receive_data=request.json
        tag = receive_data['TAG']
    except:
        abort(404)
    if (tag != 'cmdback'):
        abort(404)
    try:
        if (auth(receive_data['USERNAME'],receive_data['PASSWORD'])==True):
            send_data={'TAG':receive_data['TAG'],'AUTH':True,'DATA':read_cmdback_list(receive_data['uuid'],receive_data['serial'])}
        else:
            send_data={'TAG':receive_data['TAG'],'AUTH':False}
    except Exception as e:
        print("cmdback error:",e)
        abort(500)
    return json.dumps(send_data)

#函数auth用于认证客户端的账号和密码
def auth(username , password):

    connect = pymysql.Connection(
        host = _host,
        port= _post,
        user= _user,
        passwd= _passwd,
        db = _db,
        charset=_charset ) 

    cursorsql = connect.cursor()

    sql = "SELECT * FROM ZigBee_User Where username = " + "'" + username + "'" + " and password = " + "'" + password + "'"
    count = cursorsql.execute(sql)
    connect.commit()

    cursorsql.close()

    if count == 1:
        return True
    return False

#通过stype,num查找到netaddr
def istrans(stype , num):
    connect = pymysql.Connection(
        host = _host,
        port= _post,
        user= _user,
        passwd= _passwd,
        db = _db,
        charset=_charset ) 

    cursorsql = connect.cursor()

    sql_data = "SELECT * FROM ZigBee_table Where `stype`='" + str(stype) + "' AND `num`='" + str(num) + "'"
    #print(sql_data)
    count = cursorsql.execute(sql_data)
    connect.commit()
    cursorsql.close()

    '''
    sql_data = "SELECT * FROM ZigBee_111 Where `stype`='" + str(stype) + "' and `num`='" + str(num) + "' ORDER BY id DESC"
    count = cursorsql.execute(sql_data)
    connect.commit()
    cursorsql.close()
    '''

    if count != 0 :   
        return True
    else :
        return False

#通过stype,num查找到netaddr
def trans(stype, num):

    connect = pymysql.Connection(
        host = _host,
        port= _post,
        user= _user,
        passwd= _passwd,
        db = _db,
        charset=_charset ) 

    cursorsql = connect.cursor()

    netAddr = ''

    if istrans(stype, num):
        sql_data = "SELECT * FROM ZigBee_table Where `stype`='" + \
            str(stype) + "' AND `num`='" + str(num) + "'"
        count = cursorsql.execute(sql_data)
        connect.commit()
        if count != 0:
            results = cursorsql.fetchall()
            for row in results:
                netAddr = row[1]
                #print("netaddr",netAddr)
                break
        else :
            netAddr = None
        '''
        sql_data = "SELECT * FROM ZigBee_111 Where `stype`='" + str(stype) + "' and `num`='" + str(num) + "' ORDER BY id DESC"
        count = cursorsql.execute(sql_data)
        connect.commit()
        print("stype",stype)
        print("num",num)

        if count >= 1:
            results = cursorsql.fetchall()    #获取查询的所有记录  
            #遍历结果  
            for row in results :
                netAddr = row[1]
                break
        elif count == 0 :
            netAddr = None
        '''
    else:
        if stype == 'TY' or stype == 'YK':
            sql_data = "SELECT * FROM ZigBee_111 WHERE `stype`='SD' ORDER BY id DESC"
            count = cursorsql.execute(sql_data)
            connect.commit()
            results = cursorsql.fetchall()
            for row in results:
                netAddr = row[1]
                sql_data = "SELECT * FROM `ZigBee_table` WHERE 1"
                count = cursorsql.execute(sql_data)
                connect.commit()
                sql_data = "INSERT INTO `ZigBee_table`(`ID`, `netAddr`, `stype`, `num`) VALUES (" + str( count + 1 ) + ",'" + netAddr + "','" + stype +"','" + num + "')"
                cursorsql.execute(sql_data)
                connect.commit()
                break
        else:
            sql_data = "SELECT * FROM ZigBee_111 WHERE `stype`='" + stype + "' ORDER BY id DESC"
            count = cursorsql.execute(sql_data)
            connect.commit()
            results = cursorsql.fetchall()
            for row in results:
                netAddr = row[1]
                sql_data = "SELECT * FROM `ZigBee_table` WHERE 1"
                count = cursorsql.execute(sql_data)
                connect.commit()
                sql_data = "INSERT INTO `ZigBee_table`(`ID`, `netAddr`, `stype`, `num`) VALUES (" + str( count + 1) + ",'" + netAddr + "','" + stype + "','" + num + "')"
                cursorsql.execute(sql_data)
                connect.commit()
                break
    #netAddr = 'B009'

    cursorsql.close() #关闭与数据库的连接
    #print(netAddr)
    return netAddr

#将数据包装好
def get_designated_data(stype = '' , num = '' , count = 1):
    
    datalist = []
    data = {}
    if stype == '' and num == '':
        data = get_data()
        return data

    try:
        temp=mem_dict.get(stype+num)
        if temp!=None:
            data={
                'stype':stype,
                'num':num,
                'value':temp
            }
            return data
    except Exception as e:
        print("get_designated_data error:",e)
    
    connect = pymysql.Connection(
        host = _host,
        port= _post,
        user= _user,
        passwd= _passwd,
        db = _db,
        charset=_charset ) 

    cursorsql = connect.cursor()

    sql_data = "SELECT * FROM ZigBee_111 Where `stype`='" + str(stype) + "' and `num`='" + str(num) + "' ORDER BY id DESC"
    cursorsql.execute(sql_data)
    connect.commit()

    results = cursorsql.fetchall()    #获取查询的所有记录  
    flag = 1
    #遍历结果  
    for row in results :
        netAddr = row[1]
        ftype = row[2]
        stype = row[3]
        num = row[4]
        value = row[5]
        time = row[6]

        if flag == count :
            break
        else:
            flag = flag + 1

    data = {
        'netAddr' : netAddr ,
        'ftype' : ftype ,
        'stype' : stype ,
        'num' : num ,
        'value' : value ,
        'time' : str(time)
    }  

    cursorsql.close() #关闭与数据库的连接

    return data

#将数据包装好
def get_data():
    
    connect = pymysql.Connection(
        host = _host,
        port= _post,
        user= _user,
        passwd= _passwd,
        db = _db,
        charset=_charset ) 


    cursorsql = connect.cursor()

    sql = "SELECT * FROM ZigBee_111 Where 1 "
    id = cursorsql.execute(sql)  #获取最新id
    connect.commit()

    sql_data = "SELECT * FROM ZigBee_111 Where id = " + str(id)
    num = cursorsql.execute(sql_data)
    connect.commit()

    data = {}
    if num == 1:

        results = cursorsql.fetchall()    #获取查询的所有记录  
        #遍历结果  
        for row in results : 
            netAddr = row[1]
            ftype = row[2]
            stype = row[3]
            num = row[4]
            value = row[5]
            time = row[6]

        cursorsql.close() #关闭与数据库的连接

        data = {
            'netAddr' : netAddr ,
            'ftype' : ftype ,
            'stype' : stype ,
            'num' : num ,
            'value' : value ,
            'time' : str(time)
        }  

    return data

#检验命令是否合法并传送给tcpserver发送到下位机
def send_cmd(cmd):

    try:
        netAddr=trans(cmd['stype'],cmd['num'])
        buf= "&" + netAddr + cmd['ftype'] + cmd['uuid'] + cmd['serial'] + '*' + cmd['stype'] + cmd['num'] + cmd['value']
        #print(type(cmd['serial']))
        init_cmdback_list(cmd['uuid'],cmd['serial'])
        tcpserver_webserver_sock.sendto(bytes(buf, encoding="utf8"),'/tmp/tcpserver_webserver_socket.d')
        return True
    except Exception as e:
        print ("send_cmd error:",e)
        return False

def init_cmdback_list(uuid,serial):
    global uuid_num
    for i in range(10):
        if uuid_list[i]==uuid:
            serial_list[i*10+int(serial)]='null'
            return True
    if uuid_num>=9:
        uuid_num=0
    else:
        uuid_num=uuid_num+1
    uuid_list[uuid_num]=uuid
    for i in range (uuid_num*10,uuid_num*10+10):
        serial_list[i]='null'

def read_cmdback_list(uuid,serial):
    #print(uuid_list)
    #print(serial_list)
    for i in range(10):
        if uuid_list[i]==uuid:
            return serial_list[i*10+int(serial)]
    return 'null'

def recive_cmdback(uuid_list,serial_list,mem_dict):
    webserver_udpserver_sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)  
    path = '/tmp/webserver_udpserver_socket.d'  
    if os.path.exists(path):  
        os.unlink(path)  
    webserver_udpserver_sock.bind(path)
    while True:
        accept_data = str(webserver_udpserver_sock.recv(48),encoding="utf8")
        try:
            head = accept_data[0]
            netAddr = accept_data[1:5]
            ftype = accept_data[5:6]
            uuid = accept_data[6:38]
            serial = accept_data[38:39]
            reserve = accept_data[39:40]
            stype = accept_data[40:42]
            num = accept_data[42:44]
            value = accept_data[44:48]
        except Exception as e:
            print("recive_cmdback error:",e)
            continue
        try:
            if ftype=='C':
                for i in range(10):
                    if uuid_list[i]==uuid:
                        if value=="0000":
                            serial_list[i*10+int(serial)]="false"
                        elif value=="1111":
                            serial_list[i*10+int(serial)]="true"
            elif ftype=='D':
                temp_dict={stype+num:value}
                mem_dict.update(temp_dict)
        except Exception as e:
            print("recive_cmdback error:",e)
            continue
    webserver_udpserver_sock.close()
    
if __name__ == '__main__':
    
    tcpserver_webserver_sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    
    uuid_list = multiprocessing.Manager().list()
    serial_list = multiprocessing.Manager().list()
    mem_dict = multiprocessing.Manager().dict()

    for i in range(10):
        uuid_list.append('')
    for i in range(100):
        serial_list.append('null')

    p=multiprocessing.Process(target=recive_cmdback,args=(uuid_list,serial_list,mem_dict))
    p.deamon = True
    p.start()
    p.join

    app.run(host='127.0.0.1',port=8080,threaded=True)
    
