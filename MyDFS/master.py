import os
import re
import sys
import socket
import csv
import math
import time
import pickle as pkl
import importlib
from common import *
import threading
import numpy as np
import pandas as pd
from io import BytesIO

class Master:
    def __init__(self):
        self.isOccupied = False
        self.operation_box = pd.DataFrame({'start_time': [], 'operation': [], 'status': []})
        self.assign_node_pool = []
        if not os.path.exists('log.csv'):
            temp = pd.DataFrame(columns=['start_time', 'end_time', 'operation', 'status'])
            temp.to_csv('log.csv')
            # with open('log.csv', newline='') as f:
            #     f_csv = csv.writer(f)
            #     f_csv.writerow('start_time', 'end_time', 'operation', 'status')

    def run(self):
        # heart_beats = threading.Thread(target=AssignNode.heart_beats, args=(self,))
        # heart_beats.setDaemon(True)
        # heart_beats.start()
        self.listen()

    def listen(self):
        # 创建一个监听的socket
        listen_fd = socket.socket()
        try:
            # 监听端口
            listen_fd.bind(("0.0.0.0", master_port))
            listen_fd.listen(20)
            while True:
                # 等待连接，连接后返回通信用的套接字
                sock_fd, addr = listen_fd.accept()
                print("Received request from {}".format(addr))

                self.assign_node_pool.append(sock_fd)
                handle = threading.Thread(target=Master.handle, args=(self, sock_fd, addr))
                handle.setDaemon(True)
                handle.start()
                handle.join(5)
        except KeyboardInterrupt:  # 如果运行时按Ctrl+C则退出程序
            pass
        except Exception as e:  # 如果出错则打印错误信息
            print(e)
        finally:
            listen_fd.close()  # 释放连接

    def handle(self, sock_fd, addr):
        try:
            while True:
                if self.operation_box.shape[0] != 0 and time.time() - float(self.operation_box['start_time'][0]) > 120:
                    self.operation_box = self.operation_box.drop([0]).reset_index(drop=True)
                else:
                    break
            # 获取请求方发送的指令
            request = str(sock_fd.recv(BUF_SIZE), encoding='utf-8')
            request = request.split(" ")  # 指令之间使用空白符分割
            print(request)
            # 目前假定指令为[start/finish+id+操作类型+hosts+table_path+...]的形式，可按需求更改顺序
            # hosts为形如'thumm00,thumm01,thumm02'的字符串，可在client中利用','.join(host_list)生成
            cmd = request[0]

            if cmd == "start":  # 申请开始操作
                query_time = request[1]
                query = ' '.join(request[2:])
                task = request[2]
                hosts = get_hosts(request[3])
                table_path = request[4]
                response = self.add_query(query, query_time, table_path, hosts)
            elif cmd == "finish":  # 操作结束
                query_time = request[1]
                query = ' '.join(request[2:])
                response = self.finish_query(query, query_time)
            # elif cmd == "cancel": # 取消操作
            #     query_time = request[1]
            #     query = ' '.join(request[2:])
            #     response = self.cancel_query(query, query_time)

            sock_fd.send(bytes(response, encoding='utf-8'))
        except Exception as e:  # 如果出错则打印错误信息
            print(e)
        finally:
            sock_fd.close()
            self.assign_node_pool.remove(sock_fd)  # 释放连接


    def add_query(self, query, query_time, table_path, hosts):
        # client首次发出id为-1，master需要将其设置为当前时间，并返回给client，并添加信息到op_box中
        # 若isOccupied为1，表明master被占用，返回occupied+time
        # 若isOccupied为0，比对timestamp，分以下2种情况
        # 1. 该query为op_box中第一项：返回permitted + time
        # 2. 该query非op_box中第一项：返回occupied + time
        if query_time == '-1':
            query_time = str(time.time())
            if len(self.operation_box) == 0:
                info = {'start_time': query_time, 'operation': query, 'status': 'operating'}
            else:
                info = {'start_time': query_time, 'operation': query, 'status': 'queueing'}
            self.operation_box = self.operation_box.append(info, ignore_index=True)
            self.log_update(query, query_time, '', 'waiting')

        if self.isOccupied:
            response = 'occupied ' + str(query_time)
        elif query_time == self.operation_box['start_time'][0]:
            host = self.createTS(table_path, hosts)
            response = 'permitted ' + str(query_time) + ' ' + str(host)
            self.isOccupied = True
            self.operation_box.iloc[0, 2] = 'operating'
        else:
            response = 'occupied ' + str(query_time)
        print(response)
        return response

    def finish_query(self, query, query_time):
        self.isOccupied = False
        finish_time = str(time.time())
        self.operation_box = self.operation_box.drop([0]).reset_index(drop=True)
        self.log_update(query, query_time, finish_time, 'finished')
        return 'Master log successfully updated'

    # def cancel_query(self, query, query_time):
    #     finish_time = str(time.time())
    #     for i in range(self.operation_box['start_time']):
    #         if self.operation_box['start_time'][i] == query_time:
    #             self.operation_box = self.operation_box.drop([i]).reset_index(drop=True)
    #             break
    #     self.log_update(query, query_time, finish_time, 'canceled')
    #     return 'Mission canceled'

    def log_update(self, query, query_time, finish_time, status):
        with open('log.csv', 'a', newline='') as f:
            f_csv = csv.writer(f, dialect='excel')
            row = [str(query_time), str(finish_time), query, status]
            f_csv.writerows(row)

    # 给datanode发送形如'createTS table_path'的指令
    def createTS(self, table_path, hosts):
        print(hosts)
        for host in hosts:
            try:
                data_node_sock = socket.socket()
                data_node_sock.connect((host, data_node_port))
                request = "createTS {}".format(table_path)
                data_node_sock.send(bytes(request, encoding='utf-8'))
                time.sleep(0.2)
                data = data_node_sock.recv(BUF_SIZE)
                data = str(data, encoding='utf-8')
                data_node_sock.close()
                # if data == 'True':
                #     return host
                # else:
                #     print("May be error!")
                print(data)
                return host
            except Exception as e:
                print(e)
                continue

    def check_hosts(self, hosts):
        host = hosts[0]
        for i in range(len(hosts)):
            try:
                data_node_sock = socket.socket()
                data_node_sock.connect((hosts[i], data_node_port))
                data_node_sock.close()
                host = hosts[i]
                break
            except Exception as e:
                print(e)
            finally:
                pass
        return host


master = Master()
master.run()