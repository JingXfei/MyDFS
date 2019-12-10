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
        def sort_sstable(sstable):
            sstable.sort_values(by=['key'], inplace=True, ascending=True)
            return sstable

        # 一个sstable为64k，一行是不超过1k的数据,所以存储了64行数据(如果是100M,用100M/1k作为行数),一行里面放行键值和内容
        # 生成sstable并进行编码,key目前是文件名
        
        # tablet存放：1、sstable的索引 2、sstable的地址 3、sstable中的最后一行行键值，
        # 当tablet存放到4k截止
        # 默认一行内容不超过100Bytes,可以存40行
        
        # root meta 存储各个tablet表存放的位置
        
        data_pd = pd.DataFrame(columns=['key', 'content'])
        ss_pd = pd.DataFrame(columns=['ss_index', 'host_name', 'last_key'])
        tablet_pd = pd.DataFrame(columns=['tablet_index', 'host_name'])

        pd_index = 0
        sstable_index = 0
        tablet_index = 0

        for type_file in os.listdir(local_path):
            for file in os.listdir(local_path + type_file):
                for little_file in os.listdir(local_path + type_file + '/' + file):
                    little_file_path = local_path + type_file + '/' + file + '/' + little_file
                    file_type = os.path.splitext(little_file_path)[1]
                    if file_type == '.txt':
                        file_content = open(little_file_path).read()
                    elif file_type == '.png':
                        file_ = open(little_file_path, 'rb')
                        file_content = str(base64.b64encode(file_.read()))
                        file_.close()
                    else:
                        print('识别不出数据类型的数据集')
                    data_pd.loc[pd_index] = [str(little_file_path), file_content]
                    pd_index += 1
                    if pd_index == 64:
                        # 一个sstable已经生成，要排序并发送出去，同时作为一行存储在tablet中。
                        sstable=sort_sstable(data_pd)
                        #选择三个副本存放的地址
                        host_name = host_list[np.random.choice(len(host_list), dfs_replication, replace=False)]
                        #往三个地址上发送data
                        blk_path = dfs_path + ".sstable{}".format(40*tablet_index+sstable_index)
                        #精准的大小值
                        sstable.to_csv('test_size.csv',index=False)
                        length_data=os.path.getsize('test_size.csv')
                        fp=open('test_size.csv')
                        print(self.sendToHosts(host_name, blk_path, length_data, fp))
                        fp.close()
                        
                        #往tablet表中增加一行
                        last_key=sstable['key'][0]
                        ss_pd.loc[sstable_index] = [sstable_index,host_name, last_key]

                        #重新开启下一个sstable的存储
                        pd_index = 0
                        sstable_index += 1
                        
                    if sstable_index == 40:
                        #一个tablet满了，要发出去

                        ##存到root_meta_table中（存放tablet的地址）
                        host_name = host_list[np.random.choice(len(host_list), dfs_replication, replace=False)]
                        blk_path = dfs_path + ".tablet{}".format(tablet_index)
                        
                        ss_pd.to_csv('test_size.csv',index=False)
                        length_data=os.path.getsize('test_size.csv')
                        fp1=open('test_size.csv')
                        
                        #发送到各个服务器上
                        print(self.sendToHosts(host_name, blk_path, length_data, fp1))
                        fp1.close()

                        tablet_pd.loc[tablet_index] = [tablet_index, host_name]
                        sstable_index = 0
                        tablet_index += 1
                        
        #按照FAT表发送root_meta_tablet表
        for idx, row in fat.iterrows():
            # 解析host地址
            tablet_pd.to_csv('test_size.csv',index=False)
            length_data = os.path.getsize('test_size.csv')
            fp2=open('test_size.csv')
            host_str = row['host_name']
            host_names = get_hosts(host_str)
            blk_path = dfs_path + ".root_tablet{}".format(row['blk_no'])
            print(self.sendToHosts(host_names, blk_path, length_data, fp2))
            fp2.close()
            
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
        # 调用getFatItem函数得到Fat表；
        fat_pd = getFatItem(dfs_path)
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

    def queryArea(self, dfs_path, local_path_1, local_path_2 = "None"):
        # 范围查询；
        # local_path_2不为None时说明范围是[local_path_1, local_path_2];
        # 否则是查询local_path_1目录下的值；
        # 这里可以进行处理，比如查询目录时，查到哪里结束也是需要以row_key的形式进行表述的。
        # 首先反复调用query_table查到第一个；再基于叶节点之间的链式索引关系进行后续遍历。
        row_key = rowkey_encode(local_path_1)
        if local_path_2 != "None":
            row_key_2 = rowkey_encode(local_path_2)
        else:
            row_key_2 = "None"
        path = []

        # 调用getFatItem函数得到Fat表；并处理fat表不存在的情况
        fat = getFatItem(dfs_path)
        if fat == "None":
            print("No this Fat!")
            return None
        print("Next table: \n{}".format(fat))
        fat = pd.read_csv(StringIO(fat))
        path.append(fat)

        # 三层查询位置
        root_tablet = queryTable(fat, 0, dfs_path, row_key)
        root_tablet = str(root_tablet, encoding='utf-8')
        if root_tablet == "None":
            print("Bigger than Biggest, area not in this BigTable")
            return None
        print("Next table: \n{}".format(root_tablet))
        root_tablet = pd.read_csv(StringIO(root_tablet))
        path.append(root_tablet)

        # 经过上述过程，得知row_key必定可以存在于该树的结构中(即可以添加到某个叶节点中)
        # ——>查询tablet——>查询sstable:
        tablet = queryTable(root_tablet, 1, dfs_path, row_key) # 得到tablet中对应的一行；
        tablet = str(tablet, encoding='utf-8')
        print("Next table: \n{}".format(tablet))
        tablet = pd.read_csv(StringIO(tablet))
        path.append(tablet)

        while True:
            res,nex = queryTableArea(tablet, dfs_path, row_key, row_key_2) # 得到sstable中一个区域的内容；
            print("Result: \n{}".format(res)) # 一次只打印一个sstable的，再将该空间重新利用
            nex = pd.read_csv(StringIO(nex))
            if nex.shape[0] == 0:
                break
            else:
                tablet = pd.DataFrame(columns=['ss_index', 'host_name'])
                tablet.iloc[0] = [int(nex.iloc[0]['key'][len("sstable"):]), nex.iloc[0]['content']]

    def queryOne(self, dfs_path, local_path, bloom_filter = True):
        # 这个是一个完整的、单独用于单点查询的函数，范围查询另写。
        # 返回值有三种情况：
        # 1. None：bloomFilter校验失败，大于存储最大lastkey
        # 2.[
        #    [blk_no, host_name, blk_size], Fat表的一行；
        #    [tablet_index, host_name, last_key], rootTablet的一行；
        #    [ss_index, host_name, last_key], tablet的一行；
        #    [key, content] 最终的内容和行键
        #   ] # 若查询成功，则返回对应row_key的，否则返回大于该row_key的第一个的。

        row_key = rowkey_encode(local_path)
        path = []

        # 调用getFatItem函数得到Fat表；并处理fat表不存在的情况
        fat = getFatItem(dfs_path)
        if fat == "None":
            print("No this Fat!")
            return path
        print("Next table: \n{}".format(fat))
        fat = pd.read_csv(StringIO(fat))
        path.append(fat)

        if bloom_filter:
            # 先进行bloomFilter，
            # 若校验失败即文件不存在，返回None；
            res_filter = bloomFilter(fat, dfs_path, row_key)
            res_filter = str(res_filter, encoding='utf-8')
            if res_filter == "None":
                print("Not exist in this BigTable")
                return path

        # 若校验成功，分情况讨论：
        #   row_key小于整棵树最大的last_key，则返回None；
        #   否则返回rootTablet中对应的Tablet所在一行。
        root_tablet = queryTable(fat, 0, dfs_path, row_key) # 得到root_tablet中对应的一行；
        root_tablet = str(root_tablet, encoding='utf-8')
        if root_tablet == "None":
            print("Bigger than Biggest, area not in this BigTable")
            return path
        print("Next table: \n{}".format(root_tablet))
        root_tablet = pd.read_csv(StringIO(root_tablet))
        path.append(root_tablet)

        # 经过上述过程，得知row_key必定可以存在于该树的结构中(即可以添加到某个叶节点中)
        # ——>查询tablet——>查询sstable:
        tablet = queryTable(root_tablet, 1, dfs_path, row_key) # 得到tablet中对应的一行；
        tablet = str(tablet, encoding='utf-8')
        print("Next table: \n{}".format(tablet))
        tablet = pd.read_csv(StringIO(tablet))
        path.append(tablet)

        sstable = queryTable(tablet, 2, dfs_path, row_key) # 得到sstable中对应的一行；
        content = str(sstable, encoding='utf-8')
        print("Content: \n{}".format(content))
        sstable = pd.read_csv(StringIO(sstable))
        path.append(sstable)

        print(path)
        return path

    def bloomFilter(self, fat, dfs_path, row_key):
        # 依据要查询的row_key生成两个index，传入bloomfilter所在位置进行校验；
        index_1 = hash1(row_key)
        index_2 = hash2(row_key)

        end = '.bloom_filter'

        row = fat.iloc[0]
        host_str = row['host_name']
        host_names = get_hosts(host_str)
        for host in host_names:
            try:
                time.sleep(0.2)
                data_node_sock = socket.socket()
                print("bloomFilter from "+host+"...",end='')
                data_node_sock.connect((host, data_node_port))
                table_path = dfs_path + end
                # 传输查询的table的路径、在B+树中的层数、行键
                request = "bloomFilter {} {} {}".format(table_path, index_1, index_2)
                data_node_sock.send(bytes(request, encoding='utf-8'))
                time.sleep(0.2)  # 两次传输需要间隔一段时间，避免粘包
                res = data_node_sock.recv(BUF_SIZE)
                data_node_sock.close()
                break
            except:
                data_node_sock.close()
                print(host+" error!")
                if host == host_names[-1]:
                    return "All Error!"
                continue
        return res

    def queryTable(self, cur_table, cur_layer, dfs_path, row_key):
        if cur_layer == 0:
            end = ".root_tablet"
            ind_col = "blk_no"
        elif cur_layer == 1:
            end = ".tablet"
            ind_col = "tablet_index"
        else:
            end = ".sstable"
            ind_col = "ss_index"

        row = cur_table.iloc[0]
        host_str = row['host_name']
        host_names = get_hosts(host_str)
        for host in host_names:
            try:
                time.sleep(0.2)
                data_node_sock = socket.socket()
                print("get table from "+host+"...",end='')
                data_node_sock.connect((host, data_node_port))
                table_path = dfs_path + end + "{}".format(row[ind_col])
                # 传输查询的table的路径、在B+树中的层数、行键
                request = "query {} {} {}".format(table_path, cur_layer, row_key)
                data_node_sock.send(bytes(request, encoding='utf-8'))
                time.sleep(0.2)  # 两次传输需要间隔一段时间，避免粘包
                table = data_node_sock.recv(BUF_SIZE)
                data_node_sock.close()
                break
            except:
                data_node_sock.close()
                print(host+" error!")
                if host == host_names[-1]:
                    return "All Error!"
                continue
        return table

    def queryTableArea(self, cur_table, dfs_path, row_key_begin, row_key_end = "None"):
        # 对指定的sstable（包含范围第一个row_key)（及后续），获取范围
        # 如果end为"None"，则在datanode中按照ls的逻辑进行查询；否则找begin和end之间的数据
        res = ""
        row = cur_table.iloc[0]
        host_str = row['host_name']
        host_names = get_hosts(host_str)
        for host in host_names:
            try:
                time.sleep(0.2)
                data_node_sock = socket.socket()
                print("get table from "+host+"...",end='')
                data_node_sock.connect((host, data_node_port))
                table_path = dfs_path + ".sstable{}".format(row['ss_index'])
                # 传输查询的table的路径、开始行键、结束行键
                request = "queryArea {} {} {}".format(table_path, row_key_begin, row_key_end)
                data_node_sock.send(bytes(request, encoding='utf-8'))
                length_data = int(data_node_sock.recv(BUF_SIZE))
                # 接收数据
                ################################
                size_rec = 0
                while size_rec < length_data:
                    data = data_node_sock.recv(BUF_SIZE)
                    size_rec = size_rec + len(data)
                    data = str(data, encoding='utf-8')
                    res = res+data
                ################################
                next_sstable = data_node_sock.recv(BUF_SIZE)
                next_sstable = str(next_sstable, encoding='utf-8')
                data_node_sock.close()
                break
            except:
                data_node_sock.close()
                print(host+" error!")
                if host == host_names[-1]:
                    return "All Error!"
                continue
        return (res,next_sstable)

    def insert(self, local_path, dfs_path):
        # 此处应该调用行键编码函数进行编码 #################################################
        row_key = rowkey_encode(local_path) # 相对路径？绝对路径？
        insert_path = self.queryOne(dfs_path, local_path, bloom_filter = False)
        

    def getFatItem(self, dfs_path):
        request = "get_fat_item {}".format(dfs_path)
        print("Request: {}".format(request))
        
        # 从NameNode获取一张FAT表
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        fat_pd = self.name_node_sock.recv(BUF_SIZE)
        
        # 打印FAT表，并使用pandas读取
        fat_pd = str(fat_pd, encoding='utf-8')

        return fat_pd

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
elif cmd == "-sscopyFromLocal":
    if argc == 3:
        local_path = argv[2]
        dfs_path = argv[3]
        client.sscopyFromLocal(local_path, dfs_path)
    else:
        print("Usage: python client.py -copyFromLocal <local_path> <dfs_path>")
elif cmd == "-query":
    if argc == 3:
        dfs_path = argv[2]
        local_path = argv[3]
        client.queryOne(dfs_path, local_path)
    else:
        print("Usage: python client.py -query <dfs_path> <filename_str>")
elif cmd == "-queryArea":
    if argc == 4:
        dfs_path = argv[2]
        local_path_1 = argv[3]
        local_path_2 = argv[4]
        client.queryOne(dfs_path, local_path_1, local_path_2)
    else:
        print("Usage: python client.py -queryArea <dfs_path> <local_path_begin> <local_path_end>")
elif cmd == "-ssls":
    if argc == 3:
        dfs_path = argv[2]
        local_path = argv[3]
        client.queryArea(dfs_path, local_path)
    else:
        print("Usage: python client.py -ssls <dfs_path> <local_path>")
elif cmd == "-insert":
    if argc == 2:
        local_path = argv[2]
        dfs_path = argv[3]
        client.insert(local_path, dfs_path)
    else:
        print("Usage: python client.py -insert <local_path> <dfs_path>")
else:
    print("Undefined command: {}".format(cmd))
    print("Usage: python client.py <-ls | -copyFromLocal | -copyToLocal | -rm | -format> other_arguments")
