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

def get_hosts(host_str):
    # 由字符串找到hosts，如"['thumm01','thumm02','thumm03']"中找到三个
    name_str = np.asarray(host_str.split('\''))
    if len(name_str) == 1:
        return []
    index = np.array([2*i+1 for i in range(len(name_str)//2)])
    host_names = name_str[index]
    return host_names
