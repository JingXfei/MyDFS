import random
import os
def check_folder(folder):
    """检查文件文件夹是否存在，不存在则创建"""
    if not os.path.exists(folder):
        os.mkdir(folder)
        print(folder)
    else:
        if not os.path.isdir(folder):
            os.mkdir(folder)
    
#生成1000个小文件，每个小文件100个数，一共500M的文件
for i in range(10):
    check_folder('little_data/txt_type/data%d'%i)
    for j in range (1000):
        pageFile = open('little_data/txt_type/data%d/data%d_%d.txt' % (i,i,j), 'a')
        for k in range(50):
            pageFile.write(str(random.uniform(i, i+1)) + '\n')
        pageFile.close()

