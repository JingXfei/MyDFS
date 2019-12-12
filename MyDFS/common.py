import numpy as np
import pandas as pd
dfs_replication = 3
dfs_blk_size = 64 * 1024 * 1024

# NameNode和DataNode数据存放位置
name_node_dir = "./dfs/name"
data_node_dir = "./dfs/data"

map_reduce_code_dir = '/.code/'
map_reduce_res_dir = '/.res/'

data_node_port = 11309  # DataNode程序监听端口
name_node_port = 21309  # NameNode监听端口

# 集群中的主机列表
# host_list = ['localhost']  
host_list = np.array(['thumm01', 'thumm02', 'thumm03', 'thumm04', 'thumm05']) #, 'thumm06', 'thumm07', 'thumm08']
name_node_host = "thumm01"

PIECE_SIZE = 8 * 1024 # 一次发送8kB，方便整除；
# BUF_SIZE = 16 * 1024 # 由于socket一次只能发送最多14kB，故设为该值可接受所有数据
BUF_SIZE = dfs_blk_size * 2

# 心跳频率
heart_T = 5
# 最大无心跳计数
heart_max_count = 10 # 50s
rebuild_max_count = 3

# 最大行数和最小行数（包含sstable最后一行）
MIN_ROW = 32
MAX_ROW = 64

# TabletServer的最大等待时长
MAX_WAIT = 300

def get_hosts(host_str):
    # 由字符串找到hosts，如"['thumm01','thumm02','thumm03']"中找到三个
    name_str = np.asarray(host_str.split('\''))
    if len(name_str) == 1:
        return []
    index = np.array([2*i+1 for i in range(len(name_str)//2)])
    host_names = name_str[index]
    return host_names

def get_name_splited(local_path):
    # sstable0 3, sstable0 B, ...
    index = local_path.split('!')[-1]
    if (index[0] > '0' and index[0] < '9') or (index[0] > 'A' and index[0] < 'F'):
        temp = index[-1]
        if temp == '1':
            local_path_0 = local_path[:-1] + '0'
            local_path_1 = local_path[:-1] + '2'
        elif temp == '5':
            local_path_0 = local_path[:-1] + '4'
            local_path_1 = local_path[:-1] + '6'
        elif temp == '9':
            local_path_0 = local_path[:-1] + '8'
            local_path_1 = local_path[:-1] + 'A'
        elif temp == 'D':
            local_path_0 = local_path[:-1] + 'C'
            local_path_1 = local_path[:-1] + 'E'
        elif temp == '3':
            local_path_0 = local_path[:-1] + '1'
            local_path_1 = local_path[:-1] + '5'
        elif temp == 'B':
            local_path_0 = local_path[:-1] + '9'
            local_path_1 = local_path[:-1] + 'D'
        else:
            local_path_0 = local_path + '3'
            local_path_1 = local_path + 'B'
    else:
        local_path_0 = local_path + '!3'
        local_path_1 = local_path + '!B'

    return (local_path_0, local_path_1)