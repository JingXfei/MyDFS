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
        # self.assign_node_pool = []
        if not os.path.exists('log.csv'):
            with open('log.csv', newline='') as f:
                f_csv = csv.writer(f)
                f_csv.writerow('start_time', 'end_time', 'operation', 'status')
"""
    def heart_beats(self):
        local_name = socket.gethostname()
        while True:
            time.sleep(heart_T)
            # print("Dong~ Dong~ Dong~")
            try:
                # 5s发送一次心跳。
                name_node_sock = socket.socket()
                name_node_sock.connect((name_node_host, name_node_port))
                # 向NameNode发送心跳
                cmd = "heart_beats {}".format(local_name)
                name_node_sock.send(bytes(cmd, encoding='utf-8'))
                response_msg = name_node_sock.recv(BUF_SIZE)
                name_node_sock.close()
            except Exception as e:
                print(e)
"""
    def run(self):
        self.listen()

    def listen(self):
        # 创建一个监听的socket
        listen_fd = socket.socket()
        try:
            # 监听端口
            listen_fd.bind(("0.0.0.0", assign_node_port))
            listen_fd.listen(20)
            while True:
                # 等待连接，连接后返回通信用的套接字
                sock_fd, addr = listen_fd.accept()
                print("Received request from {}".format(addr))

                # self.assign_node_pool.append(sock_fd)
                handle = threading.Thread(target=AssignNode.handle, args=(self, sock_fd, addr))
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
                # ??? 作用是什么
                if self.operation_box.shape[0] != 0 and time.time() - float(self.operation_box['start_time'][0]) > 120:
                    self.operation_box = self.operation_box.drop([0]).reset_index(drop=True)
                else:
                    break
            # 获取请求方发送的指令
            request = str(sock_fd.recv(BUF_SIZE), encoding='utf-8')
            request = request.split(" ")  # 指令之间使用空白符分割
            print(request)

            cmd = request[0]  # 指令第一个为指令类型

            if cmd == "start":  # 申请开始操作
                query_time = request[1]
                query = ' '.join(request[2:])
                response = self.add_query(query, query_time)
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
            # self.assign_node_pool.remove(sock_fd)  # 释放连接


    def add_query(self, query, query_time):
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
            response = 'permitted ' + str(query_time)
            self.isOccupied = True
            self.operation_box.iloc[0, 2] = 'operating'
        else:
            response = 'occupied ' + str(query_time)
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

    # 还需要增加的必要功能：
    # 1. 识别query的具体操作，并向DN通知[cmd, local_path]; 目前的cmd有query和insert，但是都只需要告诉DN读取对应的tablet即可
    # 3. 接收返回的内容，再返回给Client，作为对Client一次成功的调度。
    # 2. 接收DN返回值[timestamp, isfinish]，作为对应任务完成标志。
    # 可能增加的功能：
    # 1. 冲突检测：如果两个操作所面向的host不同，这两个操作应该是可以同时进行的；


master = Master()
master.run()