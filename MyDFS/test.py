# 这是一个代码语法测试文件，请忽略
from multiprocessing import Process,Pipe
import os
import time
import pandas as pd
import numpy as np
import re

class test:
    def __init__(self):
        self.a = ['test',2]
    def run(self, conn):
        self.conn = conn
        self.conn.send(['111', self.a])
        print(self.conn.recv())

a,b = Pipe()
t = test()
p = Process(target = t.run, args =(a,))
p.start()
data = b.recv()
print(data[1])
str1 = 'log:asdead:afsrfr:adedsd sadf e f'
b.send(str1)
# cmd = str1.split(":")[0]
# print(str1[len(cmd)+1:])

# a = pd.DataFrame()
# a['test'] = [1,2,3,4,5,1,2,3,4,5]
# a.index = [3,6,2,7,2]
# a.iloc[3]['test'] = 111
# b = {}
# b['a'] = a
# b['a'].iloc[4,0] = 100
# insert = pd.DataFrame({'test': [0]})
# temp2 = data.iloc[2:]
# a = pd.concat([temp1,insert,temp2], ignore_index=True)
# b['a'] = a
# new = pd.DataFrame({'ss_index': ['a'], 'host_name': [np.array(['asd','asd'])], 'last_key': ['b']})
# print(new.iloc[[0]])

# a = pd.DataFrame()
# a['test'] = [1,2,3,4,5]
# b = a.iloc[3]
# b['test'] = 111
# print(a)


def get_name_splited(local_path):
    index = local_path.split('!')[-1]
    if (index[0] >= '0' and index[0] <= '9') or (index[0] >= 'A' and index[0] <= 'F'):
        temp = index[-1]
        if temp in ['0', '2', '4', '6', '8', 'A', 'C', 'E']:
            local_path_0 = local_path + '3'
            local_path_1 = local_path + 'B'
        elif temp == '1':
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
        local_path_0 = local_path + '!3'
        local_path_1 = local_path + '!B'
        
    return (local_path_0, local_path_1)

# a = './data_0.txt.sstable0'
# temp = [a]
# result = []
# for i in range(5):
#     pool = temp
#     result.append(pool)
#     temp = []
#     for i in pool:
#         r1,r2 = get_name_splited(i)
#         temp.append(r1)
#         temp.append(r2)
# # print(result)
# # pool.append(a)
# # pool.append('./data_0.txt.sstable0+1')
# print(np.sort(np.asarray(pool)))
# print(pool[-1].split('.')[-1][len('sstable'):])

# end = a[a.test < 10].index.tolist()[-1]
# print(a.iloc[end+1:].shape[0])