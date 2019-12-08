import os
import re
import sys
import socket
import math
import time
import pickle as pkl
import importlib
from common import *
import threading
import numpy as np
import pandas as pd
from io import BytesIO


# DataNode支持的指令有:
# 1. load 加载数据块
# 2. store 保存数据块
# 3. rm 删除数据块
# 4. format 删除所有数据块

class DataNode:        
    def __init__(self):
        self.data_node_pool = []

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
                response = self.store(sock_fd, blk_path, length_data, host_str, n)
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
                response = self.send(blk_path, length_data, host, host_str, n)
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
    
    def store(self, sock_fd, blk_path, length_data, host_str, n):
        # 1. 接收并存储数据到当前host
        local_path = data_node_dir + blk_path
        # 若目录不存在则创建新目录
        os.system("mkdir -p {}".format(os.path.dirname(local_path)))
        f = open(local_path, "wb")
        size_rec = 0
        while size_rec < length_data:
            chunk_data = sock_fd.recv(BUF_SIZE)
            size_rec = size_rec + len(chunk_data)
            # 将数据块写入本地文件
            f.write(chunk_data)
        f.close()
        print("size_rec: ",size_rec)
        # 2. 发送给下一个host
        res = ""
        host_names = get_hosts(host_str)
        while True:
            if n+1 < len(host_names):
                host = host_names[n+1]
                print("next is " + host + ", the ",n+1," in " + host_str)
                res_new, success = self.send(blk_path, length_data, host, host_str, n+1)
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

    def send(self, blk_path, length_data, host, host_str, n):
        try:
            # 1. 建立连接，发送信号
            # 向一个地址发送数据：
            print("store to "+host+"...", end=" ")
            data_node_sock = socket.socket()
            data_node_sock.connect((host, data_node_port))
            request = "store {} {} {} {}".format(blk_path,length_data,host_str,n)
            # 发送的信息包括路径、数据大小、数据流路径、你是当前第几个
            data_node_sock.send(bytes(request, encoding='utf-8'))
            time.sleep(0.2)  # 两次传输需要间隔一段时间，避免粘包

            # 2. 发送数据
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

# 创建DataNode对象并启动
data_node = DataNode()
data_node.run()
