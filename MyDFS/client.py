import os
import sys
import socket
import time
import math
from io import StringIO

import pandas as pd
import numpy as np

from common import *

class Client:
    def __init__(self):
        self.name_node_sock = socket.socket()
        self.name_node_sock.connect((name_node_host, name_node_port))
    
    def __del__(self):
        self.name_node_sock.close()
    
    def ls(self, dfs_path):
        # 向NameNode发送请求，查看dfs_path下文件或者文件夹信息
        try:
            cmd = "ls {}".format(dfs_path)
            self.name_node_sock.send(bytes(cmd, encoding='utf-8'))
            response_msg = self.name_node_sock.recv(BUF_SIZE)
            print(str(response_msg, encoding='utf-8'))
        except Exception as e:
            print(e)
        finally:
            pass
    
    def copyFromLocal(self, local_path, dfs_path):
        # 数据切分思路参考：https://cloud.tencent.com/developer/article/1481807
        file_size = os.path.getsize(local_path)
        print("File size: {}".format(file_size))
        
        request = "new_fat_item {} {}".format(dfs_path, file_size) 
        print("Request: {}".format(request))
        
        # 从NameNode获取一张FAT表
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        fat_pd = self.name_node_sock.recv(BUF_SIZE)
        
        # 打印FAT表，并使用pandas读取
        fat_pd = str(fat_pd, encoding='utf-8')
        print("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))
        
        # 根据FAT表逐个向目标DataNode发送数据块
        fp = open(local_path)
        for idx, row in fat.iterrows():
            length_data = int(row['blk_size'])
            # 解析host地址
            host_str = row['host_name']
            host_names = get_hosts(host_str)
            blk_path = dfs_path + ".blk{}".format(row['blk_no'])
            print(self.sendToHosts(host_names, blk_path, length_data, fp))
        fp.close()

    def sendToHosts(self, host_names, blk_path, length_data, fp):
        host_str = str(host_names).replace(' ', '')
        for host in host_names:
            try:
                # 向一个地址发送数据：
                print("store to "+host+"...", end=" ")
                data_node_sock = socket.socket()
                data_node_sock.connect((host, data_node_port))
                # blk_path = dfs_path + ".blk{}".format(row['blk_no'])
                request = "store {} {} {} {}".format(blk_path,length_data,host_str,0)
                # 发送的信息包括路径、数据大小、数据流路径、你是当前第几个
                data_node_sock.send(bytes(request, encoding='utf-8'))
                time.sleep(0.2)  # 两次传输需要间隔一段时间，避免粘包
                
                # 目前每次都建立连接，应该有绑定连接的方式
                surplus = length_data
                loop = math.ceil(length_data/PIECE_SIZE)
                for i in range(loop):
                    surplus = length_data - i*PIECE_SIZE
                    if surplus >= PIECE_SIZE:
                        data = fp.read(PIECE_SIZE)
                    else:
                        data = fp.read(surplus)
                    data_node_sock.sendall(bytes(data, encoding='utf-8'))
                print("send done...")
                res = data_node_sock.recv(BUF_SIZE)
                data_node_sock.close()
                return str(res, encoding='utf-8')
            except:
                print("send to "+host+" error! Maybe it break or disconnect! ")
                continue
        return "fail"
    
    def copyToLocal(self, dfs_path, local_path):
        request = "get_fat_item {}".format(dfs_path)
        print("Request: {}".format(request))
        
        # 从NameNode获取一张FAT表
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        fat_pd = self.name_node_sock.recv(BUF_SIZE)
        
        # 打印FAT表，并使用pandas读取
        fat_pd = str(fat_pd, encoding='utf-8')
        print("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))
        
        # 根据FAT表逐个从目标DataNode请求数据块，写入到本地文件中
        fp = open(local_path, "w")
        for idx, row in fat.iterrows():
            length_data = int(row['blk_size'])
            # 解析host地址
            host_str = row['host_name']
            host_names = get_hosts(host_str)
            for host in host_names:
                try:
                    time.sleep(0.2)
                    data_node_sock = socket.socket()
                    print("get data from "+host+"...",end='')
                    data_node_sock.connect((host, data_node_port))
                    blk_path = dfs_path + ".blk{}".format(row['blk_no'])
                    request = "load {}".format(blk_path)
                    data_node_sock.send(bytes(request, encoding='utf-8'))
                    time.sleep(0.2)  # 两次传输需要间隔一段时间，避免粘包
                    # loop = math.ceil(dfs_blk_size/PIECE_SIZE)
                    size_rec = 0
                    while size_rec < length_data:
                        data = data_node_sock.recv(BUF_SIZE)
                        size_rec = size_rec + len(data)
                        data = str(data, encoding='utf-8')
                        fp.write(data)
                    data_node_sock.close()
                    print("end with size:",size_rec)
                    break
                except:
                    data_node_sock.close()
                    print(host+" error!")
                    continue
        fp.close()
    
    def rm(self, dfs_path):
        request = "rm_fat_item {}".format(dfs_path)
        print("Request: {}".format(request))
        
        # 从NameNode获取改文件的FAT表，获取后删除
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        fat_pd = self.name_node_sock.recv(BUF_SIZE)
        
        # 打印FAT表，并使用pandas读取
        fat_pd = str(fat_pd, encoding='utf-8')
        print("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))
        
        # 根据FAT表逐个告诉目标DataNode删除对应数据块
        for idx, row in fat.iterrows():
            data_node_sock = socket.socket()
            data_node_sock.connect((row['host_name'], data_node_port))
            blk_path = dfs_path + ".blk{}".format(row['blk_no'])
            
            request = "rm {}".format(blk_path)
            data_node_sock.send(bytes(request, encoding='utf-8'))
            response_msg = data_node_sock.recv(BUF_SIZE)
            print(str(response_msg, encoding='utf-8'))
            
            data_node_sock.close()
    
    def format(self):
        request = "format"
        print("Request: {}".format(request))
        
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        print(str(self.name_node_sock.recv(BUF_SIZE), encoding='utf-8'))
        
        for host in host_list:
            data_node_sock = socket.socket()
            data_node_sock.connect((host, data_node_port))
            
            data_node_sock.send(bytes("format", encoding='utf-8'))
            print(str(data_node_sock.recv(BUF_SIZE), encoding='utf-8'))
            
            data_node_sock.close()

    def mapReduce(self, map_file, reduce_file, dfs_path):
        map_size = os.path.getsize(map_file)
        reduce_size = os.path.getsize(reduce_file)
        # 注意，local_path意味着结果存到本地而非DFS，不需要发送给namenode. 可以再考虑和改进！！！
        # # 暂时不更改逻辑为由client发送代码给data_node，
        request = "mapReduce {} {} {}".format(map_size, reduce_size, dfs_path)
        print("Request: {}".format(request))
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        job_id = self.name_node_sock.recv(BUF_SIZE)
        job_id = str(job_id, encoding='utf-8')
        
        # 在这一部分写代码分发到hosts的内容，基于job_id; 不过，那我为什么要把代码提交给namenode呢？？？
        blk_path_map = map_reduce_code_dir + 'map_'+job_id+'.py'
        fp = open(map_file)
        self.sendToHosts(host_list, blk_path_map, map_size, fp)
        fp.close()
        blk_path_reduce = map_reduce_code_dir + 'reduce_'+job_id+'.py'
        fp = open(reduce_file)
        self.sendToHosts(host_list, blk_path_reduce, reduce_size, fp)
        fp.close()
        
        request = "store done!"
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        result = self.name_node_sock.recv(BUF_SIZE)
        result = str(result, encoding='utf-8')
        print("result: \n{}".format(result))

# 解析命令行参数并执行对于的命令
argv = sys.argv
argc = len(argv) - 1

client = Client()

cmd = argv[1]
if cmd == '-ls':
    if argc == 2:
        dfs_path = argv[2]
        client.ls(dfs_path)
    else:
        print("Usage: python client.py -ls <dfs_path>")
elif cmd == "-rm":
    if argc == 2:
        dfs_path = argv[2]
        client.rm(dfs_path)
    else:
        print("Usage: python client.py -rm <dfs_path>")
elif cmd == "-copyFromLocal":
    if argc == 3:
        local_path = argv[2]
        dfs_path = argv[3]
        client.copyFromLocal(local_path, dfs_path)
    else:
        print("Usage: python client.py -copyFromLocal <local_path> <dfs_path>")
elif cmd == "-copyToLocal":
    if argc == 3:
        dfs_path = argv[2]
        local_path = argv[3]
        client.copyToLocal(dfs_path, local_path)
    else:
        print("Usage: python client.py -copyToLocal <dfs_path> <local_path>")
elif cmd == "-format":
    client.format()
elif cmd == "-mapReduce":
    if argc == 4:
        map_file = argv[2]
        reduce_file = argv[3]
        dfs_path = argv[4]
        client.mapReduce(map_file, reduce_file, dfs_path)
    else:
        print("Usage: python client.py -mapReduce <map_file> <reduce_file> <dfs_path>")
else:
    print("Undefined command: {}".format(cmd))
    print("Usage: python client.py <-ls | -copyFromLocal | -copyToLocal | -rm | -format> other_arguments")
