import math
import os
import socket
import time
import threading

import numpy as np
import pandas as pd
import pickle as pkl
from io import BytesIO

from common import *


# NameNode功能
# 1. 保存文件的块存放位置信息
# 2. ls ： 获取文件/目录信息
# 3. get_fat_item： 获取文件的FAT表项
# 4. new_fat_item： 根据文件大小创建FAT表项
# 5. rm_fat_item： 删除一个FAT表项
# 6. format: 删除所有FAT表项

class NameNode:
    def __init__(self):
        self.name_node_pool = []
        self.counter = np.zeros(len(host_list)) # 心跳计数
        self.rebuild_counter = np.zeros(len(host_list)) # 重建次数计数：若一次重建不成功，反复尝试n次，若仍不成功，放弃。
        self.mapreduce_job = [] # 储存当前job_id
        self.mapper_result = {} # 储存maper当前运行数量
        self.reducer_result = {} # 储存reducer当前运行数量
        self.mapper_hosts = {} # 储存真实运行mapper成功的host
        self.reducer_hosts = {} # 储存真实运行reducer成功的host

    def run(self):
        # 创建计数进程用于心跳检测使用
        count = threading.Thread(target=NameNode.count_beats, args=(self,))
        count.setDaemon(True)
        count.start()
        self.listen()

    def listen(self):
        # 创建一个监听的socket
        listen_fd = socket.socket()
        try:
            # 监听端口
            listen_fd.bind(("0.0.0.0", name_node_port))
            listen_fd.listen(20)
            print("Name node started")
            while True:
                # 等待连接，连接后返回通信用的套接字
                sock_fd, addr = listen_fd.accept()
                # print("connected by {}".format(addr)) # 把输出屏蔽掉，避免心跳信号干扰其他主要连接的debug
                # 留存疑问，线程池？不同线程相同名称？
                self.name_node_pool.append(sock_fd)
                handle = threading.Thread(target=NameNode.handle, args=(self,sock_fd,addr))
                handle.setDaemon(True)
                handle.start()
                handle.join(5)
        except KeyboardInterrupt:  # 如果运行时按Ctrl+C则退出程序
            pass
        except Exception as e:  # 如果出错则打印错误信息
            print(e)
        finally:
            listen_fd.close()  # 释放连接

    def handle(self, sock_fd, addr):  # 启动NameNode
        try:
            # 获取请求方发送的指令
            request = str(sock_fd.recv(128), encoding='utf-8')
            request = request.split()  # 指令之间使用空白符分割
            
            cmd = request[0]  # 指令第一个为指令类型
            if cmd != "heart_beats":
                print("Request: {}".format(request))
            
            if cmd == "heart_beats": # 如果是data_node心跳信号
                host_from = request[1]
                response = self.heart_beats(host_from)
            elif cmd == "ls":  # 若指令类型为ls, 则返回DFS上对于文件、文件夹的内容
                dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                response = self.ls(dfs_path)
            elif cmd == "get_fat_item":  # 指令类型为获取FAT表项
                dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                response = self.get_fat_item(dfs_path)
            elif cmd == "new_fat_item":  # 指令类型为新建FAT表项
                dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                file_size = int(request[2])
                response = self.new_fat_item(dfs_path, file_size)
            elif cmd == "rm_fat_item":  # 指令类型为删除FAT表项
                dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                response = self.rm_fat_item(dfs_path)
            elif cmd == "format":
                response = self.format()
            elif cmd == "mapReduce":
                map_size = int(request[1])
                reduce_size = int(request[2])
                dfs_path = request[3]
                response = self.map_reduce(sock_fd, map_size, reduce_size, dfs_path)
            else:  # 其他位置指令
                response = "Undefined command: " + " ".join(request)
            
            if cmd != "heart_beats":
                print("Response: {}".format(response))
            sock_fd.send(bytes(response, encoding='utf-8'))
        except Exception as e:  # 如果出错则打印错误信息
            print(e)
        finally:
            sock_fd.close()  
            self.name_node_pool.remove(sock_fd)# 释放连接

    def heart_beats(self, host_from):
        index = np.where(host_list == host_from)[0][0]
        if self.counter[index] >= 0:
            self.counter[index] = 0
        else: # 坏掉节点重新连入，格式化其中数据
            try:
                data_node_sock = socket.socket()
                data_node_sock.connect((host_from, data_node_port))
                data_node_sock.send(bytes("format", encoding='utf-8'))
                print(str(data_node_sock.recv(BUF_SIZE), encoding='utf-8'))
                data_node_sock.close()
                self.counter[index] = 0
            except Exception as e:
                print(e)
        return "Get! "

    def count_beats(self):
        while True:
            time.sleep(heart_T)
            # print(self.counter)
            index = np.where(self.counter >= 0)[0] # 只对不计作故障的节点计数
            self.counter[index] = self.counter[index] + 1
            if np.sum(self.counter > heart_max_count) > 0:
                index = np.where(self.counter > heart_max_count)[0]
                for i in index:
                    if self.rebuild_counter[i] < rebuild_max_count:
                        # 超过最大重建次数，放弃
                        check = threading.Thread(target=NameNode.check, args=(self, i))
                        check.start()
                        check.join(5)
                    else:
                        print("Give up to rebuild "+host_list[i])
                self.counter[index] = -1 # 计作故障

    def check(self, i):
        success = 1
        host_break = host_list[i]
        print(host_break+' break! ')
        # 搜集涉及损坏的节点的数据路径
        bad_data = pd.DataFrame(columns = ['host_good', 'host_new', 'dfs_path', 'blk_no', 'blk_size'])
        all_data = []
        for root,dirs,data in os.walk(name_node_dir):
            if root == name_node_dir + '/.log':
                continue
            for d in data:
                all_data.append(os.path.join(root,d))
        # 这里的路径path是真实路径
        dfs_path = [i[len(name_node_dir):] for i in all_data]

        for path in dfs_path:
            local_path = name_node_dir + path
            temp = pd.read_csv(local_path)
            for idx, row in temp.iterrows():
                host_names = get_hosts(row['host_name'])
                if host_break in host_names:
                    try:
                        host_good = [i for i in host_names if i != host_break]
                        # 在非故障的其他节点中随机选一个
                        host_new = np.random.choice([host_list[i] for i in range(len(host_list)) if host_list[i] not in host_names and self.counter[i] != -1],1)
                        bad_data.loc[bad_data.shape[0]] = [host_good,host_new,path,row['blk_no'],row['blk_size']]
                        if(self.rebuild(bad_data.loc[bad_data.shape[0]-1])):
                            temp.loc[idx] = [row['blk_no'], np.array(host_good + [host_new[0]]), row['blk_size'], row['offset']]
                        else:
                            success = 0
                    except Exception as e:
                        # 这里的错误信息主要是只有两个节点却要备份三份的情况！！！
                        print(e)
                        return None
                else:
                    continue
            # 更新目录树信息
            print(temp)
            temp.to_csv(local_path, index = False)

        # 保存日志
        local_path = name_node_dir + '/.log/' + host_break + time.strftime("_%Y-%m-%d-%H-%M-%S", time.localtime())+'.csv'
        os.system("mkdir -p {}".format(os.path.dirname(local_path)))
        bad_data.to_csv(local_path, index=False)
        print("log down!")
        if not success:
            self.counter[i] = heart_max_count - 1 # 重建没有完全成功，恢复计数重新尝试。
            self.rebuild_counter[i] = self.rebuild_counter[i] + 1 # 重建计数累计
        else:
            self.rebuild_counter[i] = 0 # 清空重建计数

    def rebuild(self, row):
        # 重建备份
        # for idx, row in bad_data.iterrows():
        print("Repair "+row['dfs_path']+'...')
        for host in row['host_good']:
            # 尝试第一个host能否发送成功
            try:
                print(host)
                # 依据要发送的数据大小决定发送次数并循环发送。
                length_data = row['blk_size']
                loop = math.ceil(length_data/PIECE_SIZE)
                # 向一个地址发送数据：
                dfs_path = row['dfs_path']
                data_node_sock = socket.socket()
                data_node_sock.connect((host, data_node_port))

                print("connect successfully!")

                blk_path = dfs_path + ".blk{}".format(row['blk_no'])
                # 发送的信息包括目标host，路径，数据大小
                request = "send {} {} {}".format(row['host_new'][0], blk_path, length_data)
                data_node_sock.send(bytes(request, encoding='utf-8'))
                time.sleep(0.2)  # 两次传输需要间隔一段时间，避免粘包
                res = data_node_sock.recv(BUF_SIZE)
                data_node_sock.close()
                print(str(res, encoding='utf-8'))
                return True
            except Exception as e:
                print(e)
                print("send to "+host+" error! Maybe it break or disconnect! ")
                continue
        return False

    def ls(self, dfs_path):
        local_path = name_node_dir + dfs_path
        # 如果文件不存在，返回错误信息
        print(local_path)
        if not os.path.exists(local_path):
            return "No such file or directory: {}".format(dfs_path)
        
        if os.path.isdir(local_path):
            # 如果目标地址是一个文件夹，则显示该文件夹下内容
            dirs = os.listdir(local_path)
            response = " ".join(dirs)
        else:
            # 如果目标是文件则显示文件的FAT表信息------和get_fat_item函数的区别？
            with open(local_path) as f:
                response = f.read()
                # read函数，显示结果如何？------
        
        return response
    
    def get_fat_item(self, dfs_path):
        # 获取FAT表内容
        local_path = name_node_dir + dfs_path
        response = pd.read_csv(local_path)
        return response.to_csv(index=False)
    
    def new_fat_item(self, dfs_path, file_size):
        # file_size是作为参数输入的？------
        nb_blks = int(math.ceil(file_size / dfs_blk_size))
        print(file_size, nb_blks)
        
        # offset为偏置, 目前还没用
        data_pd = pd.DataFrame(columns=['blk_no', 'host_name', 'blk_size', 'offset'])
        
        for i in range(nb_blks):
            blk_no = i
            # 在好的节点中选择
            host_name = np.random.choice([host_list[j] for j in range(len(host_list)) if self.counter[j] != -1], size=dfs_replication, replace=False)
            # 无放回采样，0？应该需要存备份数量个------
            blk_size = min(dfs_blk_size, file_size - i * dfs_blk_size)
            if i == 0:
                offset = 0
            else:
                offset = data_pd.loc[i-1,'offset'] + data_pd.loc[i-1, 'blk_size']
            data_pd.loc[i] = [blk_no, host_name, blk_size, offset]
        
        # print("TEST!\n"+"data_pd:",data_pd)
        
        # 获取本地路径
        local_path = name_node_dir + dfs_path
        
        # 若目录不存在则创建新目录
        os.system("mkdir -p {}".format(os.path.dirname(local_path)))
        # 保存FAT表为CSV文件
        data_pd.to_csv(local_path, index=False)
        # 同时返回CSV内容到请求节点
        return data_pd.to_csv(index=False)
    
    def rm_fat_item(self, dfs_path):
        local_path = name_node_dir + dfs_path
        response = pd.read_csv(local_path)
        os.remove(local_path)
        return response.to_csv(index=False)
    
    def format(self):
        format_command = "rm -rf {}/*".format(name_node_dir)
        os.system(format_command)
        return "Format namenode successfully~"

    def map_reduce(self, sock_fd, map_size, reduce_size, dfs_path):
        local_path = name_node_dir + dfs_path
        # 存储map和reduce代码，以时间戳作为hash命名
        job_id = str(int(time.time()))
        sock_fd.send(bytes(job_id, encoding='utf-8'))
        self.mapreduce_job.append(job_id)
        print("create job: ", job_id)
        res = sock_fd.recv(BUF_SIZE)
        print(str(res))

        mapper_list = []
        fat_pd = pd.read_csv(local_path)
        for idx, row in fat_pd.iterrows():
            host_name = get_hosts(row['host_name'])
            mapper_id = str(row['blk_no'])
            blk_path = dfs_path + ".blk{}".format(row['blk_no'])
            # 默认：多少个blk，设定多少个mapper
            mapper_list.append([mapper_id, host_name, blk_path, row['blk_size']])
        length_mapper = len(mapper_list)
        self.mapper_result[job_id] = length_mapper
        self.mapper_hosts[job_id] = [[],[]]
        if length_mapper > 1:
            for i in range(length_mapper-1):
                hosts_next_blk = mapper_list[i+1][1]
                map_worker = threading.Thread(target=NameNode.map_worker, 
                                                args=(self, job_id, mapper_list[i], hosts_next_blk))
                print("Create thread: ",mapper_list[i])
                map_worker.start()
        hosts_next_blk = []
        map_worker = threading.Thread(target=NameNode.map_worker, 
                                        args=(self, job_id, mapper_list[length_mapper-1], hosts_next_blk))
        print("Create thread: ",mapper_list[length_mapper-1])                                
        map_worker.start()

        while self.mapper_result[job_id] != 0:
            # 等待所有mapper结束
            pass
        print("Map finished!")

        reducer_list = []
        reducer_num = 1 # 默认设置为1
        max_reducer = len(host_list) - np.sum(self.counter == -1) # 最大ruducer的数量为可行节点的数目
        if reducer_num > max_reducer:
            reducer_num = max_reducer
        # 使reducer分散，随机选num个
        for i in range(reducer_num):
            reducer_id = str(i)
            host = np.random.choice([host_list[j] for j in range(len(host_list)) if self.counter[j] != -1], size=reducer_num, replace=False)
            reducer_list.append([reducer_id, host])
        length_reducer = len(reducer_list)
        self.reducer_result[job_id] = length_reducer
        self.reducer_hosts[job_id] = [[],[]]
        for i in range(length_reducer):
            reduce_worker = threading.Thread(target=NameNode.reduce_worker, 
                                                args=(self, job_id, reducer_list[i]))
            print("Create thread: ",reducer_list[i])                                   
            reduce_worker.start()
            reduce_worker.join()
        while self.reducer_result[job_id] != 0:
            # 等待所有mapper结束
            pass
        print("Reduce finished!")

        # 汇总数据
        data = []
        print(len(self.reducer_hosts[job_id][0]))
        for i in range(len(self.reducer_hosts[job_id][0])):
            try:
                r_id = self.reducer_hosts[job_id][0][i]
                host = self.reducer_hosts[job_id][1][i]
                # get map result
                reduce_res = b''
                data_node_sock = socket.socket()
                data_node_sock.connect((host, data_node_port))
                blk_path = map_reduce_res_dir + 'reduce_' + str(job_id) + '_' + str(r_id) + '.pkl'
                request = 'get {} byte {}'.format(blk_path, 0)
                data_node_sock.send(bytes(request, encoding='utf-8'))
                data_size = data_node_sock.recv(BUF_SIZE)
                geted = 0
                while geted < int(data_size):
                    temp = data_node_sock.recv(BUF_SIZE)
                    geted = geted + len(temp)
                    reduce_res = reduce_res + temp
                reduce_df = pkl.load(BytesIO(reduce_res))
                print(reduce_df)
                data.append(reduce_df)
                print(data_node_sock.recv(BUF_SIZE))
            except Exception as e:
                print("reduce: ",r_id,host,e)
                return "fail"
                # continue
            
        data_kv = pd.concat(data)
        print(data_kv)

        # 删除代码文件
        # os.remove(map_local_path) 
        # os.remove(reduce_local_path) 
        return data_kv.to_csv(index=True)

    def map_worker(self, job_id, mapper, hosts_next_blk):
        # [mapper_id, host_name, blk_path, row['blk_size']]
        mapper_id = mapper[0]
        host_name = mapper[1]
        blk_path = mapper[2]
        length_data = mapper[3]
        hosts_next_str = str(hosts_next_blk).replace(' ', '')
        print(mapper_id+' is running!')
        for host in host_name:
            try:
                data_node_sock = socket.socket()
                data_node_sock.connect((host, data_node_port))
                request = 'map {} {} {} {} {}'.format(job_id, mapper_id, blk_path, length_data, hosts_next_str)
                data_node_sock.send(bytes(request, encoding='utf-8'))
                res = data_node_sock.recv(BUF_SIZE)
                print(str(res))
                self.mapper_hosts[job_id][0].append(mapper_id)
                self.mapper_hosts[job_id][1].append(host)
                break
            except Exception as e:
                print(e)
                continue
        self.mapper_result[job_id] = self.mapper_result[job_id] - 1

    def reduce_worker(self, job_id, reducer):
        # reducer: [reducer_id, host[i]]
        reducer_id = reducer[0]
        host_name = reducer[1]
        mapper_id = str(self.mapper_hosts[job_id][0]).replace(' ', '')
        mapper_hosts = str(self.mapper_hosts[job_id][1]).replace(' ', '')
        
        for host in host_name:
            try:
                print(host)
                data_node_sock = socket.socket()
                data_node_sock.connect((host, data_node_port))
                request = 'reduce {} {} {} {}'.format(job_id, reducer_id, mapper_id, mapper_hosts)
                data_node_sock.send(bytes(request, encoding='utf-8'))
                res = data_node_sock.recv(BUF_SIZE)
                print(str(res))
                self.reducer_hosts[job_id][0].append(reducer_id)
                self.reducer_hosts[job_id][1].append(host)
                break
            except Exception as e:
                print(e)
                continue
        self.reducer_result[job_id] = self.reducer_result[job_id] - 1

# 创建NameNode并启动
name_node = NameNode()
name_node.run()
