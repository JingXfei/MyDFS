dfs_replication = 3

dfs_blk_size =4*1024*1024  # * 1024

# NameNode和DataNode数据存放位置
name_node_dir = "./dfs/name"
data_node_dir = "./dfs/data"

data_node_port = 11319  # DataNode程序监听端口
name_node_port = 21319  # NameNode监听端口,改成学号后四位,1319
reduce_node_port=31319  # Reduce监听窗口

# 集群中的主机列表
host_list = ['thumm01', 'thumm02', 'thumm03', 'thumm04', 'thumm05',
            'thumm06']

reduce_list= ['thumm01', 'thumm02','thumm03']

RE_PER_TIME=1024*8
name_node_host = "localhost"

BUF_SIZE = dfs_blk_size * 2 #8192

