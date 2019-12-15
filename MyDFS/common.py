import numpy as np
import pandas as pd
dfs_replication = 3
dfs_blk_size = 1 * 1024 * 1024

# NameNode和DataNode数据存放位置
name_node_dir = "./MyDFS/dfs/name"
data_node_dir = "./MyDFS/dfs/data"

map_reduce_code_dir = '/.code/'
map_reduce_res_dir = '/.res/'

data_node_port = 11319  # DataNode程序监听端口
name_node_port = 21319  # NameNode监听端口
master_port = 26319 # Master监听端口

# 集群中的主机列表
# host_list = ['localhost']  
host_list = np.array(['thumm01', 'thumm02', 'thumm03', 'thumm04', 'thumm05']) #, 'thumm06', 'thumm07', 'thumm08']
name_node_host = "thumm01"
master_host = "thumm01"

PIECE_SIZE = 8 * 1024 # 一次发送8kB，方便整除；
# BUF_SIZE = 16 * 1024 # 由于socket一次只能发送最多14kB，故设为该值可接受所有数据
BUF_SIZE = dfs_blk_size * 2

# 心跳频率
heart_T = 5
# 最大无心跳计数
heart_max_count = 50 # 50s
rebuild_max_count = 3

# 最大行数和最小行数（包含sstable最后一行）
MIN_ROW = 32
MAX_ROW = 64

# TabletServer的最大等待时长
MAX_WAIT = 10

def get_hosts(host_str):
    # 由字符串找到hosts，如"['thumm01','thumm02','thumm03']"中找到三个
    name_str = np.asarray(host_str.split('\''))
    if len(name_str) == 1:
        return []
    index = np.array([2*i+1 for i in range(len(name_str)//2)])
    host_names = name_str[index]
    return host_names

def get_name_splited(index):
    # sstable0 3, sstable0 B, ...
    temp = index.split('!')
    if len(temp) > 1:
        end = temp[-1][-1]
        if end == '1':
            index_0 = index[:-1] + '0'
            index_1 = index[:-1] + '2'
        elif end == '5':
            index_0 = index[:-1] + '4'
            index_1 = index[:-1] + '6'
        elif end == '9':
            index_0 = index[:-1] + '8'
            index_1 = index[:-1] + 'A'
        elif end == 'D':
            index_0 = index[:-1] + 'C'
            index_1 = index[:-1] + 'E'
        elif end == '3':
            index_0 = index[:-1] + '1'
            index_1 = index[:-1] + '5'
        elif end == 'B':
            index_0 = index[:-1] + '9'
            index_1 = index[:-1] + 'D'
        else:
            index_0 = index + '3'
            index_1 = index + 'B'
    else:
        index_0 = index + '!3'
        index_1 = index + '!B'

    return (index_0, index_1)

# def rowkey_encode():
#     # 行键编码函数
#     pass

# def hash1():
#     # bloomfilter的哈希第一函数
#     pass

# def hash2():
#     # bloomfilter的哈希第二函数
#     pass