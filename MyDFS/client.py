import os
import socket
import time
from io import StringIO
import pandas as pd
import random
import re
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
        file_size = os.path.getsize(local_path)
        print("File size: {}".format(file_size))
        print('buf_size',BUF_SIZE)
        request = "new_fat_item {} {} {}".format(local_path,dfs_path, file_size)
        print("Request: {}".format(request))

        # 从NameNode获取一张FAT表
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        fat_pd = self.name_node_sock.recv(BUF_SIZE)

        # 打印FAT表，并使用pandas读取
        fat_pd = str(fat_pd, encoding='utf-8')
        print("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))
        fp = open(local_path)
        for idx, row in fat.iterrows():

            data = fp.read(int(row['blk_size']))
            data_bytes=bytes(data,encoding='utf-8')
            #************************************
            for host in range(dfs_replication):
                # ******************************
                host_name=row['host_name'][2+host*10:9+host*10]
                data_node_sock = socket.socket()        #####每次都要新建一个，>不能是放在外面的
                data_node_sock.connect((host_name, data_node_port))
                # *******************************
                blk_path = dfs_path + ".blk{}".format(row['blk_no'])
                # blk_path = dfs_path + ".blk{}".format(row['blk_no'])

                request = "store {}".format(blk_path)
                data_node_sock.send(bytes(request, encoding='utf-8'))
                time.sleep(0.2)  # 两次传输需要间隔一段时间，避免粘包
                
                # gap_abs = int(row['blk_size']) % 1024
                # count = int(row['blk_size']) // 1024
                #
                # data_node_sock.send(bytes(str(count), encoding='utf-8'))
                # time.sleep(0.2)  # 两次传输需要间隔一段时间，避免粘包
                #
                # for i in range(count):
                #     data_node_sock.send(data_bytes[1024*i:1024*i+1024], socket.MSG_WAITALL)
                # data_node_sock.send(data_bytes[-gap_abs:],socket.MSG_WAITALL)
                count=int(int(row['blk_size'])/RE_PER_TIME)
                gap=int(row['blk_size'])-RE_PER_TIME*count
                #gap_0_send=bytes(str(-1000),encoding='utf-8')
                data_node_sock.send(bytes(str(count),encoding='utf-8'))
                time.sleep(0.2)
                data_node_sock.send(bytes(str(gap),encoding='utf-8'))
                time.sleep(0.2)
                for i in range (count):
                    data_node_sock.send(data_bytes[RE_PER_TIME*i:RE_PER_TIME*i+RE_PER_TIME])
                if gap!=0:
                    data_node_sock.send(data_bytes[-gap:])
                #else:
                #     data_node_sock.send(gap_0_send)
                time.sleep(0.2)
              
                print(data_node_sock.recv(BUF_SIZE))
                data_node_sock.close()

        fp.close()
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
        fp = open(local_path, "wb")
        for idx, row in fat.iterrows():
            data_node_sock = socket.socket()
            data_node_sock.connect((row['host_name'][2:9], data_node_port))
            blk_path = dfs_path + ".blk{}".format(row['blk_no'])
            request = "load {}".format(blk_path)
            print(request,'request_copy_to')
            data_node_sock.send(bytes(request, encoding='utf-8'))
            time.sleep(0.2)  # 两次传输需要间隔一段时间，避免粘包

            data_count = data_node_sock.recv(1024)
            print('输出data_count', data_count, type(data_count), str(data_count))
            count_1 = str(data_count)[2:]
            count = int(count_1[:-1])
            print('count', count)
            time.sleep(0.2)

            data_gap = data_node_sock.recv(1024)
            # print(data_count,type(data_count),str(data_count))
            gap_1 = str(data_gap)[2:]
            gap = int(gap_1[:-1])
            print('gap', gap)
            time.sleep(0.2)
            
            chunk_data = b''
            for i in range(count):
                data = data_node_sock.recv(RE_PER_TIME, socket.MSG_WAITALL)
                chunk_data += data
                #if i==0:
                #    print('chunk_data',chunk_data)
            time.sleep(0.2)
            if gap != 0:
                chunk_data += data_node_sock.recv(gap, socket.MSG_WAITALL)
            print('接收完毕')
            time.sleep(0.2)
            
  
            print(data_node_sock.recv(BUF_SIZE))
            #data = str(chunk_data, encoding='utf-8')
            fp.write(chunk_data)
            data_node_sock.close()
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
            #*****************************************
            for host in range(dfs_replication):
                data_node_sock = socket.socket()
                #**************************************
                data_node_sock.connect((row['host_name'][2+host*10:9+host*10], data_node_port))
                blk_path = dfs_path + ".blk{}".format(row['blk_no'])

                request = "rm {}".format(blk_path)
                data_node_sock.send(bytes(request, encoding='utf-8'))
                response_msg = data_node_sock.recv(BUF_SIZE)
                print(response_msg)

                data_node_sock.close()

    def format(self):
        request = "format"
        print(request)
    
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        print(str(self.name_node_sock.recv(BUF_SIZE), encoding='utf-8'))
    
        for host in host_list:
            data_node_sock = socket.socket()
            data_node_sock.connect((host, data_node_port))
        
            data_node_sock.send(bytes("format", encoding='utf-8'))
            print(str(data_node_sock.recv(BUF_SIZE), encoding='utf-8'))
        
            data_node_sock.close()
    

    def client_reduce(self,dfs_path):
        request = "get_fat_item {}".format(dfs_path)
        print("Request: {}".format(request))
        # 从NameNode获取一张FAT表
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        fat_pd = self.name_node_sock.recv(BUF_SIZE)
    
        # 打印FAT表，并使用pandas读取
        fat_pd = str(fat_pd, encoding='utf-8')
        print("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))
    
        # 根据FAT表逐个向请求处理
        reduce_result_list=[]
        for idx, row in fat.iterrows():
            reduce_node = socket.socket()
            #随机选择一个reduce.py处理
            chosen_random_reduce=random.randint(0,len(reduce_list)-1)
            print(reduce_list[chosen_random_reduce])
            reduce_node.connect((reduce_list[chosen_random_reduce], reduce_node_port))
            #随机选择block存放的一个地址
            chosen_random_replia=random.randint(0,dfs_replication-1)
            data_path=row['host_name'][2+10*chosen_random_replia:9+10*chosen_random_replia]
            
            blk_path = dfs_path + ".blk{}".format(row['blk_no'])
            
            request = "get_data {} {}".format(data_path,blk_path)
            reduce_node.send(bytes(request, encoding='utf-8'))
            
            time.sleep(0.2)  # 两次传输需要间隔一段时间，避免粘包

            reduce_response_msg = reduce_node.recv(BUF_SIZE)
            reduce_response_msg=reduce_response_msg.decode()
            reduce_response_msg=re.sub('\[|\]|\(|\)|\'','',reduce_response_msg)
            reduce_result_list.append(eval(reduce_response_msg))
            time.sleep(0.2)
            reduce_node.close()
            
        terminal_result=self.reduce(reduce_result_list)
        print(terminal_result)

        
    def reduce(self, mapperOut):
       
        cumVal = 0.0
        cumSumSq = 0.0
        cumN = 0.0
        for instance in mapperOut:
            print('instance',instance)
            nj = float(instance[0])  # 第一个字段是数据个数
            cumN += nj
            cumVal += float(instance[1])  # 第二个字段是一个map输出的均值，均值乘以数据个数就是数据总和
            cumSumSq += float(instance[2])  # 第三个字段是一个map输出的平方和的均值，乘以元素个数就是所有元素的平方和
        mean = cumVal / cumN  # 得到所有元素的均值
        var = (cumSumSq / cumN - mean * mean)  # 得到所有元素的方差
        result = [cumN, mean, var]
        return result


# 解析命令行参数并执行对于的命令

import sys

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
        print("Usage: python client.py -rm <dfs_path>")
elif cmd == "-copyToLocal":
    if argc == 3:
        dfs_path = argv[2]
        local_path = argv[3]
        client.copyToLocal(dfs_path, local_path)
    else:
        print("Usage: python client.py -copyFromLocal <dfs_path> <local_path>")
elif cmd == "-format":
    client.format()

elif cmd=="-client_reduce":
    if argc==2:
        dfs_path=argv[2]
        client.client_reduce(dfs_path)

else:
    print("Undefined command: {}".format(cmd))
    print("Usage: python client.py <-ls | -copyFromLocal | -copyToLocal | -rm | -format> other_arguments")


