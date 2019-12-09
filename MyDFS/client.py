import os
import sys
import socket
import time
import math
from io import StringIO

import pandas as pd
import numpy as np
import base64
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

    def sscopyFromLocal(self, local_path, dfs_path):
        # python3 client.py -sscopyFromLocal little_data/ /test/test.txt
        file_size = 64*1024
    
        request = "new_fat_item {} {}".format(dfs_path, file_size)
        print("Request: {}".format(request))
    
        # 从NameNode获取一张FAT表,为指向tablet的指针
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        fat_pd = self.name_node_sock.recv(BUF_SIZE)
    
        # 打印FAT表，并使用pandas读取
        fat_pd = str(fat_pd, encoding='utf-8')
        print("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))
        
        # 新添内容
        # 对本地文件分块并发送，存储表的信息
        def get_content(little_file_path):
            file_type = os.path.splitext(little_file_path)[1]
            if file_type == '.txt':
                file_content = open(little_file_path).read()
            elif file_type == '.png':
                file_ = open(little_file_path, 'rb')
                file_content = str(base64.b64encode(file_.read()))
                file_.close()
            else:
                print('识别不出数据类型的数据集')
                file_content=None
            return file_content
        
        def sort_encode(sort_table):
            sort_table.sort_values(by=['key'], inplace=True, ascending=True)
            return sort_table
        
        def encode_file(file_path):
            return os.path.abspath(file_path)
        
        # 一个sstable为64k，一行是不超过1k的数据,所以存储了64行数据(如果是100M,用100M/1k作为行数)
        # 前63行每行里面放行键值和内容,最后一行是下一个sstable的地址
        # 生成sstable,key目前是文件名
        
        # tablet存放：1、sstable的索引 2、sstable的地址 3、sstable中的最后一行行键值，
        # 当tablet存放到4k截止
        # 默认一行内容不超过100Bytes,可以存40行
        
        # root meta存储1、tablet的索引 2、tablet的地址 3、tablet中的最后一行行键值，
        
        sstable = pd.DataFrame(columns=['key', 'content'])
        tablet = pd.DataFrame(columns=['ss_index', 'host_name', 'last_key'])
        root = pd.DataFrame(columns=['tablet_index', 'host_name','last_key'])
        host_name_tr = host_list[np.random.choice(len(host_list), dfs_replication, replace=False)]

        # 对所有文件的编码进行排序（运行时间比较长，所以先存下来了）
        # #已保存，之后如果有新的数据集或到新机器上重新运行一次
        # sort_table=pd.DataFrame(columns=['key'])
        # sort_index=0
        # for type_file in os.listdir(local_path):
        #     for file in os.listdir(local_path + type_file):
        #         for little_file in os.listdir(local_path + type_file + '/' + file):
        #             little_file_path = local_path + type_file + '/' + file + '/' + little_file
        #             sort_table.loc[sort_index]=encode_file(little_file_path)
        #             sort_index+=1
        # file_index=sort_encode(sort_table)
        # file_index.to_csv('sort_table.csv')
        
        file_sort=pd.read_csv('sort_table.csv')
        #   所有文件的长度
        all_len=len(file_sort)
        #   选择63是因为最后一行是指向下一个sstable的地址
        sstable_num=int(all_len/63)
        data_end_num=all_len-63*sstable_num
        ss_total_num=sstable_num+ bool(data_end_num/63)
        
        tablet_num=int(ss_total_num/40)         #存满的tablet_num个数
        tablet_rest=ss_total_num-tablet_num*40  #最后一个tablet中存放的sstable的个数
        tablet_total_num=tablet_num+bool(tablet_rest)
        
        flag=True

        for tabindex in range (tablet_total_num):
            tablet = pd.DataFrame(columns=['ss_index', 'host_name', 'last_key'])
            if not flag:
                break
            for ssindex in range (40):
                sstable = pd.DataFrame(columns=['key', 'content'])
                if not flag:
                    break
                for data_index in range (63):
                    #往sstable里填充内容
                    file_index=40*63*tabindex+63*ssindex+data_index
                    little_file_path = file_sort['key'][file_index]
                    if not os.path.exists(little_file_path):
                        flag=False
                        break
                    file_content=get_content(little_file_path)
                    sstable.loc[data_index]=[str(little_file_path),file_content]
                
                #如果有上一个sstable，往上一个的最后一行存放当前sstable的地址，并将上一个sstable传出去1
                if 'host_name' in locals().keys()  and 'blk_path' in locals().keys() :
                    host_name0=host_name
                    blk_path0=blk_path
                
                #选择当前sstable的地址
                host_name = host_list[np.random.choice(len(host_list), dfs_replication, replace=False)]
                blk_path = dfs_path + ".sstable{}.csv".format(40*tabindex+ssindex)
    
                # 存储到tablet
                last_key=sstable['key'][len(sstable)-1]
                tablet.loc[ssindex] = [ssindex,host_name,last_key]
                # 如果有上一个sstable，往上一个的最后一行存放当前sstable的地址，并将上一个sstable传出去2
                if 'host_name0' in locals().keys() and 'blk_path0' in locals().keys():
                    send_sstable = pd.read_csv('ss.csv')
                    send_sstable.loc[len(send_sstable)] = [ssindex, host_name]
                    send_sstable.to_csv('ss.csv', index=False)
                    length_data = os.path.getsize('ss.csv')
                    fp = open('ss.csv')
                    print(self.sendToHosts(host_name0, blk_path0, length_data, fp))
                    fp.close()

                # 将当前的sstable存放到ss.csv中(一个临时文件，不断被覆盖的)
                sstable.to_csv('ss.csv', index=False)
        
            # 当tablet存满到40行,发出去并存储到root
            blk_path_tablet = dfs_path + ".tablet{}.csv".format(tabindex)
            tablet.to_csv('tablet.csv',index=False)
            length_data=os.path.getsize('tablet.csv')
            fp_t=open('tablet.csv')
            print(self.sendToHosts(host_name_tr, blk_path_tablet, length_data, fp_t))
            root.loc[tabindex] = [tabindex,host_name_tr,last_key]
        
        #*到这里最后一个sstable还没有发送出去
        #如果正好是整数个满的sstable表，在最后一个sstable最后增加None
        if data_end_num==0:
            last_sstable = pd.read_csv('ss.csv')
            #增加一行index,None
            last_sstable.loc[len(last_sstable)] = [sstable_num, 'None']
            last_sstable.to_csv('ss.csv', index=False)
            length_data = os.path.getsize('ss.csv')
            fp1=open('ss.csv')
            print(self.sendToHosts(host_name, blk_path, length_data, fp1))
            fp1.close()
            
        # 如果不是整数个满的sstable表，在最后一个sstable增加新表的地址并发出，之后发送存储满的sstable
        # 还要在tablet之后再新加一行
        else:
            #读剩下来的sstable内容
            for rest_data_index in range (data_end_num):
                file_index=63*sstable_num+rest_data_index
                little_file_path = file_sort['key'][file_index]
                file_content=get_content(little_file_path)
                sstable.loc[rest_data_index]=[str(little_file_path),file_content]
                
            #安排剩下的sstable存放的地址
            host_name_re = host_list[np.random.choice(len(host_list), dfs_replication, replace=False)]
            blk_path_re = dfs_path + ".sstable{}.csv".format(ssindex)
            
            #增加一行,host_name_re，并发送
            last_sstable = pd.read_csv('ss.csv')
            last_sstable.loc[len(last_sstable)] = [sstable_num, host_name_re]
            last_sstable.to_csv('ss.csv', index=False)
            length_data = os.path.getsize('ss.csv')
            fp2=open('ss.csv')
            print(self.sendToHosts(host_name, blk_path, length_data, fp2))
            fp2.close()
            
            #在最后的sstable后面增加一行None ,发送最后的sstable
            last_key=sstable['key'][len(sstable)-1]
            sstable['key'][len(sstable)]=['end','None']
            
            sstable.to_csv('ss.csv', index=False)
            length_data = os.path.getsize('ss.csv')
            fp3=open('ss.csv')
            print(self.sendToHosts(host_name_re, blk_path_re, length_data, fp3))
            fp3.close()
            
            # 将最后的sstable存入到tablet表中
            tablet.loc[len(tablet)] = [sstable_num,host_name_re,last_key]
            root.loc[len(root)]=[tablet_total_num,'aitat',last_key]
            #发送最后的root和tablet
            tablet.to_csv('tabletend.csv',index=False)
            root.to_csv('root.csv',index=False)
            blk_path_tr = dfs_path + ".tablet{}.csv".format(tablet_total_num)
            blk_path_fr = dfs_path + ".root{}.csv".format(0)
            fpt=open('tabletend.csv')
            fpr=open('root.csv')
            print(self.sendToHosts(host_name_tr, blk_path_tr, length_data, fpt))
            print(self.sendToHosts(host_name_tr, blk_path_fr, length_data, fpr))

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
elif cmd=="-sscopyFromLocal":
    if argc == 3:
        local_path = argv[2]
        dfs_path = argv[3]
        client.sscopyFromLocal(local_path, dfs_path)
    else:
        print("Usage: python client.py -sscopyFromLocal <local_path> <dfs_path>")

else:
    print("Undefined command: {}".format(cmd))
    print("Usage: python client.py <-ls | -copyFromLocal | -copyToLocal | -rm | -format> other_arguments")
