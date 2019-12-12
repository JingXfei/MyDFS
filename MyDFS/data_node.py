import os
import re
import sys
import socket
import math
import time
import pickle as pkl
import importlib
from common import *
import numpy as np
import pandas as pd
from io import BytesIO

from multiprocessing import Process, Pipe # 多进程，进程之间双向通信
import subprocess
import threading

# DataNode支持的指令有:
# 1. load 加载数据块
# 2. store 保存数据块
# 3. rm 删除数据块
# 4. format 删除所有数据块

class DataNode:        
    def __init__(self):
        self.DN_end,self.TS_end = Pipe() # 用于进程之间通信
        self.data_node_pool = []
        self.tablet_server = 0 # 判断是否存在一个tabletServer

    def run(self):
        heart_beats = threading.Thread(target=DataNode.heart_beats, args=(self,))
        heart_beats.setDaemon(True)
        heart_beats.start()
        self.listen()

    def listen(self):
        # 创建一个监听的socket
        listen_fd = socket.socket()
        try:
            # 监听端口
            listen_fd.bind(("0.0.0.0", data_node_port))
            listen_fd.listen(20)
            while True:
                # 等待连接，连接后返回通信用的套接字
                sock_fd, addr = listen_fd.accept()
                print("Received request from {}".format(addr))

                self.data_node_pool.append(sock_fd)
                handle = threading.Thread(target=DataNode.handle, args=(self,sock_fd,addr))
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
            # 获取请求方发送的指令
            request = str(sock_fd.recv(BUF_SIZE), encoding='utf-8')
            request = request.split(" ")  # 指令之间使用空白符分割
            print(request)
            
            cmd = request[0]  # 指令第一个为指令类型

            if cmd == "load":  # 加载数据块
                blk_path = request[1]  # 指令第二个参数为DFS目标地址
                response = self.load(sock_fd, blk_path)
            elif cmd == "store":  # 存储数据块
                blk_path = request[1]  # 指令第二个参数为DFS目标地址
                length_data = int(request[2])
                host_str = request[3]
                n = int(request[4])
                if len(request) == 5:
                    response = self.store(sock_fd, blk_path, length_data, host_str, n)
                else:
                    mode = request[5]
                    response = self.store(sock_fd, blk_path, length_data, host_str, n, mode)
            elif cmd == "rm":  # 删除数据块
                blk_path = request[1]  # 指令第二个参数为DFS目标地址
                response = self.rm(blk_path)
            elif cmd == "format":  # 格式化DFS
                response = self.format()
            elif cmd == "send":  # 向另一个data_node发送数据
                host = request[1]
                blk_path = request[2]
                length_data = int(request[3])
                host_str = str([host])
                n = 0
                if len(request) == 4:
                    response = self.send(blk_path, length_data, host, host_str, n)
                else:
                    mode = request[4]
                    response = self.send(blk_path, length_data, host, host_str, n, mode)
            elif cmd == "map":
                # job_id, mapper_id, blk_path, length_data, hosts_next_str
                job_id = request[1]
                mapper_id = request[2]
                blk_path = request[3]
                length_data = int(request[4])
                hosts_next_str = request[5]
                response = self.map(job_id, mapper_id, blk_path, length_data, hosts_next_str)
            elif cmd == "reduce":
                # job_id, reducer_id, mapper_id, mapper_hosts
                job_id = request[1]
                reducer_id = request[2]
                mapper_id = request[3]
                mapper_hosts = request[4]
                response = self.reduce(job_id, reducer_id, mapper_id, mapper_hosts)
            elif cmd == "get":
                blk_path = request[1]
                read_model = request[2]
                size = int(request[3])
                response = self.get(sock_fd, blk_path, read_model, size)
            elif cmd == "query":
                # table_path, cur_layer, row_key
                table_path = request[1]
                cur_layer = int(request[2])
                row_key = request[3]
                response = self.query(table_path, cur_layer, row_key)
            elif cmd == "bloomFilter":
                # table_path, index_1, index_2, row_key
                table_path = request[1]
                index_1 = int(request[2])
                index_2 = int(request[3])
                row_key = request[4]
                response = self.bloomFilter(table_path, index_1, index_2, row_key)
            elif cmd == "queryArea":
                # table_path, row_key_begin, row_key_end
                table_path = request[1]
                row_key_begin = request[2]
                row_key_end = request[3]
                response = self.queryArea(sock_fd, table_path, row_key_begin, row_key_end)
            elif cmd == "createTS":
                # table_path
                table_path = request[1]
                response = self.CreateTS(table_path)
            elif cmd == "insert":
                table_path = request[1]
                row_key = request[2]
                timestamp = request[3]
                host_str = request[4]
                length_data = int(request[5])
                response = self.insert(sock_fd, table_path, row_key, timestamp, host_str, length_data):
            else:
                response = "Undefined command: " + " ".join(request)
            
            sock_fd.send(bytes(response, encoding='utf-8'))
        except Exception as e:  # 如果出错则打印错误信息
            print(e)
        finally:
            sock_fd.close()
            self.data_node_pool.remove(sock_fd)# 释放连接
    
    def load(self, sock_fd, blk_path):
        # 本地路径
        local_path = data_node_dir + blk_path
        # 读取本地数据
        f = open(local_path)
        loop = math.ceil(dfs_blk_size/PIECE_SIZE)
        # 循环读取并发送
        for i in range(loop-1):
            surplus = dfs_blk_size - i*PIECE_SIZE
            print("read file...")
            data = f.read(PIECE_SIZE)
            print("send...")
            sock_fd.sendall(bytes(data, encoding='utf-8'))
        data = f.read(surplus)
        f.close()
        return data
    
    def store(self, sock_fd, blk_path, length_data, host_str, n, mode = 'w'):
        # 1. 接收并存储数据到当前host
        local_path = data_node_dir + blk_path
        # 若目录不存在则创建新目录
        os.system("mkdir -p {}".format(os.path.dirname(local_path)))
        size_rec = 0
        if mode == 'w':
            f = open(local_path, "wb")
            while size_rec < length_data:
                chunk_data = sock_fd.recv(BUF_SIZE)
                size_rec = size_rec + len(chunk_data)
                # 将数据块写入本地文件
                f.write(chunk_data)
            f.close()
        else:
            content = ""
            while size_rec < length_data:
                chunk_data = sock_fd.recv(BUF_SIZE)
                size_rec = size_rec + len(chunk_data)
                content = content + chunk_data
            with open(local_path, "ab") as f:
                f.write(content)
        print("size_rec: ",size_rec)
        # 2. 发送给下一个host
        res = ""
        host_names = get_hosts(host_str)
        while True:
            if n+1 < len(host_names):
                host = host_names[n+1]
                print("next is " + host + ", the ",n+1," in " + host_str)
                if mode == 'w':
                    res_new, success = self.send(blk_path, length_data, host, host_str, n+1, mode)
                else:
                    res_new, success = self.send(blk_path, length_data, host, host_str, n+1, mode, content)
                res = res + res_new
                if success:
                    break
                else:
                    n = n+1
            else:
                break
            # except Exception as e:
            #     print(e)
            #     res = res + "send to "+ host +" error! Maybe it break or disconnect! \n"
            
        res = res + "Store chunk {} successfully~ with length_data {} in {} \n".format(local_path,size_rec,socket.gethostname())

        return res

    def send(self, blk_path, length_data, host, host_str, n, mode = 'w', content = False):
        try:
            # 1. 建立连接，发送信号
            # 向一个地址发送数据：
            print("store to "+host+"...", end=" ")
            data_node_sock = socket.socket()
            data_node_sock.connect((host, data_node_port))
            request = "store {} {} {} {} {}".format(blk_path,length_data,host_str,n,mode)
            # 发送的信息包括路径、数据大小、数据流路径、你是当前第几个
            data_node_sock.send(bytes(request, encoding='utf-8'))
            time.sleep(0.2)  # 两次传输需要间隔一段时间，避免粘包

            # 2. 发送数据
            if content == False:
                local_path = data_node_dir + blk_path
                fp = open(local_path)
                loop = math.ceil(length_data/PIECE_SIZE)
                surplus = length_data
                for i in range(loop):
                    surplus = length_data - i*PIECE_SIZE
                    if surplus >= PIECE_SIZE:
                        data = fp.read(PIECE_SIZE)
                    else:
                        data = fp.read(surplus)
                    data_node_sock.sendall(bytes(data, encoding='utf-8'))
            else:
                loop = math.ceil(length_data/PIECE_SIZE)
                surplus = length_data
                for i in range(loop):
                    surplus = length_data - i*PIECE_SIZE
                    if surplus >= PIECE_SIZE:
                        data = content[i*PIECE_SIZE:(i+1)*PIECE_SIZE]
                    else:
                        data = content[i*PIECE_SIZE:]
                    data_node_sock.sendall(bytes(data, encoding='utf-8'))
            res = data_node_sock.recv(BUF_SIZE)
            data_node_sock.close()
            fp.close()
            return str(res, encoding = "utf-8"), True
        except Exception as e:
            print(e)
            res = "send to "+ host +" error! Maybe it break or disconnect! \n"
            return res, False
    
    def rm(self, blk_path):
        local_path = data_node_dir + blk_path
        rm_command = "rm -rf " + local_path
        os.system(rm_command)
        
        return "Remove chunk {} successfully~".format(local_path)
    
    def format(self):
        format_command = "rm -rf {}/*".format(data_node_dir)
        os.system(format_command)
        
        return "Format datanode successfully~"

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
            except Exception as e:
                print(e)
            name_node_sock.close()

    def map(self, job_id, mapper_id, blk_path, length_data, hosts_next_str):
        map_local_path = data_node_dir + map_reduce_code_dir + 'map_'+job_id+'.py'
        while not os.path.exists(map_local_path):
            # 等待确认已收到map代码文件
            pass
        code_file = data_node_dir + map_reduce_code_dir
        code_file = code_file[:-1]
        if code_file not in sys.path:
            sys.path.append(code_file)
        code_name = 'map_'+job_id
        user_map = importlib.import_module(code_name) # 导入函数
        # print("import function success!")
        # 读入数据
        local_path = data_node_dir + blk_path
        blk_no = re.findall('(\d+)', blk_path)[-1]
        fp = open(local_path)
        lines = fp.readlines()
        fp.close()
        data = pd.DataFrame()
        while lines[-1] == '':
            lines = lines[:-1]
        if int(blk_no) != 0:
            lines = lines[1:]

        # print("get the last line...")
        hosts_next = get_hosts(hosts_next_str)
        
        l_no = len(blk_no)
        blk_path_next = blk_path[:-l_no] + str(int(blk_no)+1)
        last_line = ''
        for host in hosts_next:
            try:
                data_node_sock = socket.socket()
                data_node_sock.connect((host, data_node_port))
                request = 'get {} line {}'.format(blk_path_next, 1) # 按行读取1行
                data_node_sock.send(bytes(request, encoding='utf-8'))
                data_size = data_node_sock.recv(BUF_SIZE)
                geted = 0
                while geted < int(data_size):
                    # 几乎可以保证是字符串，所以直接str然后相加
                    temp = data_node_sock.recv(BUF_SIZE)
                    geted = geted + len(temp)
                    last_line = last_line + str(temp, encoding='utf-8')
                # data_node_sock.recv(BUF_SIZE)
                break
            except Exception as e:
                print(e)
                continue
        if lines[-1][-1] == '\n' and last_line != '':
            lines.append(last_line)
        else:
            lines[-1] = lines[-1] + last_line
        data['key'] = [i for i in range(len(lines))]
        data['value'] = lines
        print("last_line: ", lines[-1])
        data = data.set_index('key')
        # print("get data success!")

        result = user_map.map(data)
        # result应该也是一个dataframe，按job_id和mapper_id命名存储到固定路径，方便reduce处理
        res_local_path = data_node_dir + map_reduce_res_dir + 'map_' + str(job_id) + '_' + str(mapper_id) + '.pkl'
        os.system("mkdir -p {}".format(os.path.dirname(res_local_path)))
        result.to_pickle(res_local_path)
        # print("mapper success!")
        return job_id+"_"+mapper_id+" done!"

    def reduce(self, job_id, reducer_id, mapper_id, mapper_hosts):
        reduce_local_path = data_node_dir + map_reduce_code_dir + 'reduce_'+job_id+'.py'
        while not os.path.exists(reduce_local_path):
            # 等待确认已收到map代码文件
            pass
        code_file = data_node_dir + map_reduce_code_dir
        code_file = code_file[:-1]
        if code_file not in sys.path:
            sys.path.append(code_file)
        code_name = 'reduce_'+job_id
        user_reduce = importlib.import_module(code_name) # 导入函数

        # print("import function success!")

        # 汇总数据
        mapper_id = get_hosts(mapper_id)
        mapper_hosts = get_hosts(mapper_hosts)
        data = []
        for i in range(len(mapper_hosts)):
            try:
                # get map result
                map_res = b''
                data_node_sock = socket.socket()
                data_node_sock.connect((mapper_hosts[i], data_node_port))
                blk_path = map_reduce_res_dir + 'map_' + job_id + '_' + mapper_id[i] + '.pkl'
                request = 'get {} byte {}'.format(blk_path, 0)
                data_node_sock.send(bytes(request, encoding='utf-8'))
                data_size = data_node_sock.recv(BUF_SIZE)
                time.sleep(0.2)
                geted = 0
                while geted < int(data_size):
                    temp = data_node_sock.recv(BUF_SIZE)
                    geted = geted + len(temp)
                    map_res = map_res + temp
                map_df = pkl.load(BytesIO(map_res))
                # print(data_node_sock.recv(BUF_SIZE))
            except Exception as e:
                print(mapper_id[i],mapper_hosts[i],e)
                return "fail"
                # continue
            data.append(map_df)
        data_kv = pd.concat(data)
        # print("get data success!")

        # 运行
        result = user_reduce.reduce(data_kv)
        # result应该也是一个dataframe，按job_id和reducer_id命名存储到固定路径，方便reduce处理
        res_local_path = data_node_dir + map_reduce_res_dir + 'reduce_' + str(job_id) + '_' + str(reducer_id) + '.pkl'
        os.system("mkdir -p {}".format(os.path.dirname(res_local_path)))
        result.to_pickle(res_local_path)
        # print("reduce success!")
        return job_id+"_"+reducer_id+" done!"

    def get(self, sock_fd, blk_path, read_model, size):
        # 索要数据，需要包含路径、读取模式和读取大小，若size为0，则认为是读取全部
        # 读取模式分为行、字节
        # print("start: ")
        local_path = data_node_dir + blk_path
        file_size = os.path.getsize(local_path)
        # print("open file")
        f = open(local_path, 'rb')
        if size == 0:
            data = f.read(file_size)
        elif read_model == 'line':
            data = b''
            for i in range(size):
                temp = f.readline()
                data = data + temp
        elif read_model == 'byte':
            data = f.read(size)
        else:
            return "read_model not in ['line','byte']"
        f.close()
        # print("return size")
        true_size = len(data)
        sock_fd.send(bytes(str(true_size), encoding='utf-8'))
        time.sleep(0.2)
        loop = math.ceil(true_size/PIECE_SIZE)
        sended = 0
        # print("send")
        for i in range(loop):
            sended = i*PIECE_SIZE
            if (true_size - sended) >= PIECE_SIZE:
                temp = data[sended:(sended+PIECE_SIZE)]
            else:
                temp = data[sended:]
            # sock_fd.sendall(bytes(temp, encoding='utf-8'))
            sock_fd.sendall(temp) # temp本身是一个byte类型
        time.sleep(0.2)
        return ''

    def query(self, table_path, cur_layer, row_key):
        # sstable的最后一行是索引，所以要注意排除
        # 找到则返回，未找到返回大于他的第一个
        # 在TS中查询，查询操作不记录log
        if not self.tablet_server:
            return "May be error for there is not a Tablet Server"
        local_path = data_node_dir + table_path

        # 创建数据流
        # query:local_path, cur_layer, row_key
        data = [local_path, cur_layer, row_key]
        
        # 传输命令到TS进程
        self.DN_end.send(['query', data])
        result = self.DN_end.recv()
        return result
    
    def bloomFilter(self, table_path, index_1, index_2, row_key):
        # 必然查询的是.bloom_filter文件，该文件格式需要确定；
        local_path = data_node_dir + table_path
        file_size = os.path.getsize(local_path)
        with open(local_path) as f:
            data = pkl.load(f)
        if data[0][index_1] == 1 and data[1][index_2] == 1:
            l = len('.bloom_filter')
            return self.query(table_path[:-l]+'.root_tablet0', 0, row_key)
        else:
            return "None1"

    def queryArea(self, sock_fd, table_path, row_key_begin, row_key_end):
        # 必然查询的是sstable，
        if not self.tablet_server:
            return "May be error for there is not a Tablet Server"
        local_path = data_node_dir + table_path

        # 创建数据流
        # queryArea:local_path, row_key_begin, row_key_end
        data = [local_path, row_key_begin, row_key_end]
        
        # 传输命令到TS进程
        self.DN_end.send(['queryArea', data])
        result = self.DN_end.recv()

        # 循环发送数据；
        str_to_send = result[0].to_csv(index=False)
        length_data = len(str_to_send)
        sock_fd.send(bytes(str(length_data), encoding='utf-8'))
        time.sleep(0.2)
        loop = math.ceil(length_data/PIECE_SIZE)
        surplus = length_data
        for i in range(loop):
            surplus = length_data - i*PIECE_SIZE
            if surplus >= PIECE_SIZE:
                str_piece = str_to_send[i*PIECE_SIZE:(i+1)*PIECE_SIZE]
            else:
                str_piece = str_to_send[i*PIECE_SIZE:]
            sock_fd.sendall(bytes(str_piece, encoding='utf-8'))
        time.sleep(0.2)

        # 判断是否需要读下一个sstable
        next_judge = result[1].to_csv(index=False)
        return next_judge

    def CreateTS(self, table_path):
        # 基于master的调度生成一个新的进程；初始传入对应的table_path
        local_path = data_node_dir + table_path
        if not self.tablet_server:
            # 不存在该tablet_server，则创建一个TS
            self.TS = TabletServer()
            self.p = Process(target = self.TS.run, args = (self.TS_end,))
            self.p.start()
            self.tablet_server = 1
        try:
            self.DN_end.send(['read:', local_path])
        except Exception as e:
            print(e)
        return self.DN_end.recv()

    def insert(self, sock_fd, table_path, row_key, timestamp, host_str, length_data):
        # 插入到对应的tablet中 <k,v>，此时master应该已经指挥该datanode建立TS进程
        if not self.tablet_server:
            return "May be error for there is not a Tablet Server"
        local_path = data_node_dir + table_path

        # 接收数据
        content = ""
        size_rec = 0
        while size_rec < length_data:
            chunk_data = sock_fd.recv(BUF_SIZE)
            size_rec = size_rec + len(chunk_data)
            content = content + str(chunk_data)

        log_path = local_path + ".log"
        # 创建日志文件
        # 可能涉及的指令为：
        # [cmd: insert]: row_key, local_path, content;
        # [cmd: delete]: row_key, local_path;
        log = pd.DataFrame(columns = ['timestamp', 'cmd', 'localpath', 'row_key', 'content', 'other', 'isfinish'])
        log.iloc[0] = [timestamp, 'insert', local_path, row_key, content, host_str, 0]
        if not os.path.exists(log_path):
            mode = 'w'
            log.to_csv(log_path, index=False)
            data = log.to_csv(index=False)
        else:
            mode = 'a'
            log.to_csv(log_path, index=False, mode = 'a', header = False)
            data = log.to_csv(index=False, header = False)

        # 生成数据流路径；
        host_names = get_hosts(host_str)
        local = os.gethostname()
        n = np.where(host_names == local)[0][0]
        if n != 0:
            print("Exist an error for this is not the first host!")
            index = [n]+[i for i in range(dfs_replication) if i != n]
            host_names = host_names[index]
        host_str = str(host_names).replace(' ', '')
        self.send(table_path+".log", len(data), host_names[1], host_str, 1, mode, content = data)
        
        # 传输命令到TS进程
        self.DN_end.send(['log:', log])
        result = self.DN_end.recv()
        if result != 'Done':
            # 需要向后续节点发送新的数据
            info = pd.read_csv(StringIO(result))
            index0 = info.iloc[0]['index0']
            index1 = info.iloc[0]['index1']
            length_data = os.path.getsize(log_path)
            self.send(table_path+".log", length_data, host_names[1], host_str, 1)
            filetype = table_path.split('.')[-1]
            if filetype[0] == 's':
                l = len(filetype)
                table_path_0 = table_path[:-l]+'.sstable'+index0
                table_path_1 = table_path[:-l]+'.sstable'+index1
            else:
                l = len(filetype)
                table_path_0 = table_path[:-l]+'.tablet'+index0
                table_path_1 = table_path[:-l]+'.tablet'+index1
            length_data_0 = os.path.getsize(data_node_dir + table_path_0)
            self.send(table_path_0, length_data_0, host_names[1], host_str, 1)
            length_data_1 = os.path.getsize(data_node_dir + table_path_1)
            self.send(table_path_1, length_data_1, host_names[1], host_str, 1)
        return result

class TabletServer:
    def __init__(self):
        # 包含内存中加载的tablet、log;
        self.tablets = {}
        self.logs = {}
        self.logs_done = {}
        self.times = {}

    def run(self, TS_end):
        self.TS_end = TS_end
        while True:
            operation = self.TS_end.recv()
            print(operation)
            handle = threading.Thread(target=TabletServer.handle, args=(self,operation))
            handle.setDaemon(True)
            handle.start()
            handle.join(5)
            
    def handle(self, operation):
        cmd = operation[0]
        data = operation[1]
        if cmd == 'read':
            response = self.read_tablet(data)
        elif cmd == 'log':
            response = self.log(data)
        elif cmd == 'query':
            response = self.query(data)
        elif cmd == 'queryArea':
            response = self.query_area(data)
        else:
            response = "May be an error, here is else."
            print(response)
        self.TS_end.send(response)
    
    def read_tablet(self, local_path):
        if self.tablet.haskey(local_path):
            return True
        # 读取tablet顺便生成对应的log，一个0行的df
        self.tablets[local_path] = pd.read_csv(local_path)
        self.logs[local_path] = pd.DataFrame(columns = ['timestamp', 'cmd', 'localpath', 'row_key', 'content', 'other', 'isfinish'])
        self.logs_done[local_path] = pd.DataFrame(columns = ['timestamp', 'cmd', 'localpath', 'row_key', 'content', 'other', 'isfinish'])
        self.times[local_path] = 0
        self.check_tablet(local_path)
        # 循环检查log和记录时间的线程
        check_times = threading.Thread(target=TabletServer.check_times, args=(self,local_path))
        check_times.setDaemon(True)
        check_times.start()
        return True
    
    def check_tablet(self,local_path):
        # 用于检查对应tablet在磁盘的log，看读取的tablet是否是最新的。
        log_path = local_path + ".log"
        log_temp = pd.read_csv(log_path)
        # 检查是否本地记录的log均已完成，若未完成则读入对应的任务到内存log中
        end = log_temp[log_temp.isfinish == 0].index.tolist()[-1]
        op_df = log_temp.iloc[end+1:]
        for i in range(op_df.shape[0]):
            self.process_log(op_df.iloc[i])

    def check_times(self, local_path):
        # 循环计算与上次被访问的时间，时间过长则存回磁盘
        while True:
            cur_time = time.time()
            if cur_time - self.logs_done[local_path].iloc[-1,0] > MAX_WAIT:
                # 一定时间无操作，存回本地，并删除对应tablet
                self.store_tablet(local_path)
                self.store_log(local_path)
                del self.tablets[local_path]
                del self.logs[local_path]
                del self.logs_done[local_path]
                del self.times[local_path]
            time.sleep(10)

    def process_log(op):
        # ['timestamp', 'cmd', 'localpath', 'row_key', 'content', 'other', 'isfinish']
        timestamp = op['timestamp']
        cmd = op['cmd']
        local_path = op['localpath']
        row_key = op['row_key']
        content = op['content']
        other = op['other']
        isfinish = op['isfinish']

        if cmd == "insert":
            result = self.insert(local_path, row_key, content, other)
        else:
            result = "Done"
        n = self.logs_done[local_path].shape[0]
        self.logs_done[local_path].iloc[n] = [timestamp, cmd, local_path, row_key, content, other, 1]
        if result != "Done":
            # 确认任务完成之后，由于分裂，存储log并在内存中删除
            self.store_log(local_path)
            print(self.logs_done[local_path])
            del self.logs[local_path]
            del self.logs_done[local_path]
        return result

    def log(self, data):
        # 日志记录并进行具体操作
        op_df = data
        self.read_tablet(local_path)
        local_path = op_df.iloc[0]['localpath']
        self.logs[local_path] = pd.concat([self.logs[local_path], op_df], index=False)
        result = self.process_log(op_df.iloc[0])
        return result

    def insert(self, local_path, row_key, content, other):
        # 以local_path为键找到对应字典中的数据，根据row_key插入指定内容
        # sstable = pd.DataFrame(columns=['key', 'content'])
        # tablet = pd.DataFrame(columns=['ss_index', 'host_name', 'last_key'])
        # root = pd.DataFrame(columns=['tablet_index', 'host_name','last_key'])
        data = self.tablets[local_path].iloc[:-1]
        last = self.tablets[local_path].iloc[[-1]]
        marker = local_path.split(' ')[-1]
        if marker[0] == 's':
            # 插入的对象是sstable
            temp1 = data[data.key <= row_key]
            temp2 = data[data.key > row_key]
            new = pd.DataFrame({'key': [row_key], 'content': [content]})
            self.tablets[local_path] = pd.concat([temp1,new,temp2,last], ignore_index=True)
            if self.tablets[local_path].shape[0] > MAX_ROW:
                # 行数超过最大，需要分裂；
                # 返回信息：新生成的sstable的ss_index, last_key
                # 存储新的sstable到本地，更新本地log
                local_path_0, local_path_1 = get_name_splited(local_path)
                index0 = local_path_0.split('.')[-1][len('sstable'):]
                index1 = local_path_1.split('.')[-1][len('sstable'):]

                n = self.tablets[local_path].shape[0]
                self.tablets[local_path_0] = self.tablets[local_path].iloc[:n//2]
                self.tablets[local_path_1] = self.tablets[local_path].iloc[n//2:]
                self.logs[local_path_0] = pd.DataFrame(columns = ['timestamp', 'cmd', 'localpath', 'row_key', 'content', 'other', 'isfinish'])
                self.logs[local_path_1] = pd.DataFrame(columns = ['timestamp', 'cmd', 'localpath', 'row_key', 'content', 'other', 'isfinish'])

                last_key = self.tablets[local_path_0].iloc[-1]['key']
                host_names = get_hosts(other)
                self.tablets[local_path_0].iloc[self.tablets[local_path_0].shape[0]] = [index1, host_names]
                
                self.store_tablet(local_path_0)
                self.store_tablet(local_path_1)
                self.store_tablet(local_path) # 目前还是先保存一下
                del self.tablets[local_path]

                result = pd.DataFrame({'last_key': [last_key], 'index0': [index0], 'index1': [index1]})
                return result.to_csv(index=False)
            return "Done"
        elif marker[0] == 't':
            # 插入的对象是tablet，则需要删掉一行增加两行；
            # content(index, index0, index1)需要转换
            temp = content.split(" ")
            index_del = temp[0]
            index0 = temp[1]
            index1 = temp[2]
            # host_name = get_hosts(temp[3])
            
            temp1 = data[data.last_key < row_key]
            temp2 = data[data.last_key >= row_key]
            new = temp2.iloc[0]
            new.iloc[0,[0,2]] = [index0,row_key]
            temp2.iloc[0,0] = index1
            print("new rows 1 in tablet:\n",new)
            print("new rows 2 in tablet:\n",temp2.iloc[0])
            self.tablets[local_path] = pd.concat([temp1,new,temp2], ignore_index=True)
            if self.tablets[local_path].shape[0] > MAX_ROW:
                # 行数超过最大，需要分裂；
                # 返回信息：新生成的tablet的tablet_index, last_key
                # 存储新的tablet到本地，更新本地log
                local_path_0, local_path_1 = get_name_splited(local_path)
                index0 = local_path_0.split('.')[-1][len('tablet'):]
                index1 = local_path_1.split('.')[-1][len('tablet'):]

                n = self.tablets[local_path].shape[0]
                self.tablets[local_path_0] = self.tablets[local_path].iloc[:n//2]
                self.tablets[local_path_1] = self.tablets[local_path].iloc[n//2:]
                self.logs[local_path_0] = pd.DataFrame(columns = ['timestamp', 'cmd', 'localpath', 'row_key', 'content', 'other', 'isfinish'])
                self.logs[local_path_1] = pd.DataFrame(columns = ['timestamp', 'cmd', 'localpath', 'row_key', 'content', 'other', 'isfinish'])

                last_key = self.tablets[local_path_0].iloc[-1]['last_key']
                
                self.store_tablet(local_path_0)
                self.store_tablet(local_path_1)
                del self.tablets[local_path]

                result = pd.DataFrame({'last_key': [last_key], 'index0': [index0], 'index1': [index1]})
                return result.to_csv(index=False)
                
            return "Done"
        else:
            # 插入的对象是root_tablet
            # content(tablet_index, host_str)需要转换为tablet_index和host_name(list)
            temp = content.split(" ")
            index_del = temp[0]
            index0 = temp[1]
            index1 = temp[2]
            
            temp1 = data[data.last_key < row_key]
            temp2 = data[data.last_key >= row_key]
            new = temp2.iloc[0]
            new.iloc[0,[0,2]] = [index0,row_key]
            temp2.iloc[0,0] = index1
            print("new rows 1 in tablet:\n",new)
            print("new rows 2 in tablet:\n",temp2.iloc[0])
            self.tablets[local_path] = pd.concat([temp1,new,temp2], ignore_index=True)
            return "Done"

    def store_tablet(self, local_path):
        self.tablets[local_path].to_csv(local_path, index=False)

    def store_log(self, local_path):
        self.logs_done[local_path].to_csv(local_path+'.log', index=False, mode = 'a', header = False)

    def query(self, data):
        local_path = data[0]
        cur_layer = data[1]
        row_key = data[2]
        self.read_tablet(local_path)
        if cur_layer == 0:
            # 要查讯对应的row_key在哪一行下；
            tablet = self.tablets[local_path]
            if row_key > tablet.iloc[-1]['last_key']:
                # 大于树中存储数据的最大值；
                result = "None2"
                return result
            result = tablet[tablet.last_key >= row_key].iloc[[0]]
            tablet_id = result.iloc[0]['tablet_index']
            l = len('.root_tablet0')      
            new_path = table_path[:-l]+".tablet{}".format(tablet_id)
            ss_info = query(new_path, 1, row_key)
            ss_info = pd.read_csv(StringIO(ss_info))

            res = pd.DataFrame(columns = ['ss_index', 'host_name', 'last_key'])
            res.iloc[0] = result.iloc[0].tolist()
            res.iloc[1] = ss_info.iloc[0].tolist()
            result = res
            # return res.to_csv(index=False)
        elif cur_layer == 1:
            tablet = pd.read_csv(local_path)
            result = tablet[tablet.last_key >= row_key].iloc[[0]]
            # return result.to_csv(index=False)
        else:
            sstable = pd.read_csv(local_path)
            result = sstable[sstable.key >= row_key].iloc[[0]]
            ################################################################
            # 较大文件的query需要考虑（还未完成）                               #
            ################################################################
        return result.to_csv(index=False)

    def query_area(self, data):
        # 必然查询的是sstable，
        # local_path, row_key_begin, row_key_end
        local_path = data[0]
        row_key_begin = data[1]
        row_key_end = data[2]

        self.read_tablet(local_path)
        sstable = self.tablets[local_path]
        data = sstable.iloc[:-1]
        next_sstable = sstable.iloc[-1:]
        if row_key_end == "None":
            # 按ls的方式查询；
            l = len(row_key_begin)
            index = []
            for idx, row in data.iterrows():
                if len(row['key'])>= l and row['key'][:l] == row_key_begin:
                    index.append(idx)
            result = data.iloc[index]
        else:
            result = data[data.key >= row_key_begin]
            result = result[result.key <= row_key_end]

        # 判断是否需要读下一个sstable
        next_judge = pd.DataFrame()
        if result.shape[0] > 0:
            if result.iloc[-1]['key'] == data.iloc[-1]['key']:
                next_judge = next_sstable
        return [result, nex_judge]

# 创建DataNode对象并启动
data_node = DataNode()
data_node.run()
