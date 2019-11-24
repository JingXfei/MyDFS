##实验二：Hadoop 并行编程

###任务1：掌握 Hadoop DFS 常用指令

1. Hadoop DFS支持指令

   ```shell
   2019211309@thumm01:~$ hadoop fs
   Usage: hadoop fs [generic options]
   	[-appendToFile <localsrc> ... <dst>]
   	[-cat [-ignoreCrc] <src> ...]
   	[-checksum <src> ...]
   	[-chgrp [-R] GROUP PATH...]
   	[-chmod [-R] <MODE[,MODE]... | OCTALMODE> PATH...]
   	[-chown [-R] [OWNER][:[GROUP]] PATH...]
   	[-copyFromLocal [-f] [-p] [-l] [-d] [-t <thread count>] <localsrc> ... <dst>]
   	...
   
   Generic options supported are:
   -conf <configuration file>        specify an application configuration file
   -D <property=value>               define a value for a given property
   -fs <file:///|hdfs://namenode:port> specify default filesystem URL to use, overrides 'fs.defaultFS' property from configurations.
   -jt <local|resourcemanager:port>  specify a ResourceManager
   -files <file1,...>                specify a comma-separated list of files to be copied to the map reduce cluster
   -libjars <jar1,...>               specify a comma-separated list of jar files to be included in the classpath
   -archives <archive1,...>          specify a comma-separated list of archives to be unarchived on the compute machines
   
   The general command line syntax is:
   command [genericOptions] [commandOptions]
   ```
   
2. ```ls```指令使用：

   ```shell
   2019211309@thumm01:~$ hadoop fs -ls /
   Found 3 items
   drwxr-xr-x   - dsjxtjc    dsjxtjc             0 2019-10-21 19:47 /dsjxtjc
   drwxr-xr-x   - 2018211009 dsjxtjc             0 2019-10-19 00:03 /tmp
   -rw-r--r--   3 root       supergroup   13588590 2019-10-18 14:50 /wc_dataset.txt
   ```

   ```shell
   2019211309@thumm01:~$ hadoop fs -ls /dsjxtjc/2019211309
   2019211309@thumm01:~$
   ```

3. ```copyFromLocal```指令使用：

   创建本地文件：

   ```shell
   2019211309@thumm01:~$ touch test.txt
   2019211309@thumm01:~$ echo "Hello Hadoop" > test.txt
   2019211309@thumm01:~$ cat test.txt
   Hello Hadoop
   ```

   传输本地文件到DFS中：

   ```shell
   2019211309@thumm01:~$ hadoop fs -copyFromLocal ./test.txt /dsjxtjc/2019211309/
   2019-10-21 19:59:57,518 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
   ```

   查看DFS中文件：

   ```shell
   2019211309@thumm01:~$ hadoop fs -cat /dsjxtjc/2019211309/test.txt
   2019-10-21 20:04:56,498 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
   Hello Hadoop
   ```

###任务2：使用 MapReduce 模型进行词频统计

1. 创建数据文件

   ```shell
   2019211309@thumm01:~$ mkdir wc_input
   2019211309@thumm01:~$ cd wc_input/
   2019211309@thumm01:~/wc_input$ echo "Hello World" > test1.txt
   2019211309@thumm01:~/wc_input$ echo "Hello Hadoop" > test2.txt
   2019211309@thumm01:~/wc_input$ cat test1.txt test2.txt
   Hello World
   Hello Hadoop
   ```

2. 传输数据文件到DFS中

   ```shell
   2019211309@thumm01:~/wc_input$ hadoop fs -copyFromLocal -d ~/wc_input /dsjxtjc/2019211309/
   2019-10-21 20:21:16,702 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
   2019-10-21 20:21:16,900 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
   2019211309@thumm01:~/wc_input$ hadoop fs -ls /dsjxtjc/2019211309/wc_input
   Found 2 items
   -rw-r--r--   3 2019211309 dsjxtjc         12 2019-10-21 20:21 /dsjxtjc/2019211309/wc_input/test1.txt
   -rw-r--r--   3 2019211309 dsjxtjc         13 2019-10-21 20:21 /dsjxtjc/2019211309/wc_input/test2.txt
   ```

3. 创建并编写```WordCount.java```文件

   ```shell
   2019211309@thumm01:~/wc_input$ cd
   2019211309@thumm01:~$ mkdir -p word_count
   2019211309@thumm01:~$ cd word_count/
   2019211309@thumm01:~/word_count$ vim WordCount.java
   2019211309@thumm01:~/word_count$ hadoop com.sun.tools.javac.Main WordCount.java
   2019211309@thumm01:~/word_count$ jar cf wc.jar WordCount*.class
   ```

   ```WordCount.java```文件：

   ```java
   import java.io.IOException;
   import java.util.StringTokenizer;
   import org.apache.hadoop.conf.Configuration;
   import org.apache.hadoop.fs.Path;
   import org.apache.hadoop.io.IntWritable;
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.Job;
   import org.apache.hadoop.mapreduce.Mapper;
   import org.apache.hadoop.mapreduce.Reducer;
   import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
   public class WordCount {
   	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
   		private final static IntWritable one = new IntWritable(1);
       private Text word = new Text();
   		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
   			StringTokenizer itr = new StringTokenizer(value.toString()); while (itr.hasMoreTokens()) {
   				word.set(itr.nextToken());
   				context.write(word, one);
   			}
   		}
   	}
   	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
   		private IntWritable result = new IntWritable();
   		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
   			int sum = 0;
   			for (IntWritable val : values) {
   				sum += val.get();
   			}
   			result.set(sum);
   			context.write(key, result);
   		}
   	}
   	public static void main(String[] args) throws Exception {
   		Configuration conf = new Configuration();
   		Job job = Job.getInstance(conf, "word count"); job.setNumReduceTasks(3); //@设置reducer的数量
   		job.setJarByClass(WordCount.class);
   		job.setMapperClass(TokenizerMapper.class);
   		job.setCombinerClass(IntSumReducer.class);
   		job.setReducerClass(IntSumReducer.class);
   		job.setOutputKeyClass(Text.class);
   		job.setOutputValueClass(IntWritable.class);
   		FileInputFormat.addInputPath(job, new Path(args[0]));
   		FileOutputFormat.setOutputPath(job, new Path(args[1]));
   		System.exit(job.waitForCompletion(true) ? 0 : 1);
   	}
   }
   ```

4. 编译java程序

   ```shell
   2019211309@thumm01:~/word_count$ hadoop com.sun.tools.javac.Main WordCount.java
   2019211309@thumm01:~/word_count$ jar cf wc.jar WordCount*.class
   ```

5. 运行jar包

   ```shell
   2019211309@thumm01:~/word_count$ hadoop jar wc.jar WordCount /dsjxtjc/2019211309/wc_input /dsjxtjc/2019211309/wc_output
   2019-10-21 20:53:51,801 INFO client.RMProxy: Connecting to ResourceManager at thumm01/192.168.0.101:8032
   2019-10-21 20:53:52,695 WARN mapreduce.JobResourceUploader: Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
   2019-10-21 20:53:52,790 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/2019211309/.staging/job_1571414388168_0069
   2019-10-21 20:53:52,949 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
   2019-10-21 20:53:53,164 INFO input.FileInputFormat: Total input files to process : 2
   2019-10-21 20:53:53,204 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
   2019-10-21 20:53:53,256 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
   2019-10-21 20:53:53,270 INFO mapreduce.JobSubmitter: number of splits:2
   2019-10-21 20:53:53,420 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
   2019-10-21 20:53:53,443 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1571414388168_0069
   2019-10-21 20:53:53,443 INFO mapreduce.JobSubmitter: Executing with tokens: []
   2019-10-21 20:53:53,670 INFO conf.Configuration: resource-types.xml not found
   2019-10-21 20:53:53,671 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
   2019-10-21 20:53:53,756 INFO impl.YarnClientImpl: Submitted application application_1571414388168_0069
   2019-10-21 20:53:53,816 INFO mapreduce.Job: The url to track the job: http://thumm01:8088/proxy/application_1571414388168_0069/
   2019-10-21 20:53:53,817 INFO mapreduce.Job: Running job: job_1571414388168_0069
   2019-10-21 20:54:00,955 INFO mapreduce.Job: Job job_1571414388168_0069 running in uber mode : false
   2019-10-21 20:54:00,958 INFO mapreduce.Job:  map 0% reduce 0%
   2019-10-21 20:54:06,048 INFO mapreduce.Job:  map 100% reduce 0%
   2019-10-21 20:54:12,095 INFO mapreduce.Job:  map 100% reduce 100%
   2019-10-21 20:54:12,107 INFO mapreduce.Job: Job job_1571414388168_0069 completed successfully
   ```

6. 查看结果

   ```shell
   2019211309@thumm01:~/word_count$ hadoop fs -copyToLocal /dsjxtjc/2019211309/wc_output ./
   2019-10-21 20:56:45,359 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
   2019-10-21 20:56:45,532 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
   2019211309@thumm01:~/word_count$ ls wc_output/
   part-r-00000  part-r-00001  part-r-00002  _SUCCESS
   2019211309@thumm01:~/word_count$ cat wc_output/part-r-00000
   Hello	2
   2019211309@thumm01:~/word_count$ cat wc_output/part-r-00001
   2019211309@thumm01:~/word_count$ cat wc_output/part-r-00002
   Hadoop	1
   World	1
   ```

### 任务3：设计并实现自己的分布式文件系统

####3.1 概述：

该DFS实现了多副本存储、基础的MapReduce框架、节点宕机处理等功能。

1. 设计思想：

   该DFS主要基于助教提供的基础框架，借鉴Hadoop的设计思想完成。

   整体的DFS由一个负责记录目录树和协调资源的name_node和多个用于数据存储和分布式计算的data_node，以及用于提出需求并与name_node和data_node通信的client构成。其中，name_node和data_node以及data_node之间能够相互通信，以实现任务分配和数据移动。

2. 目录结构：

   * MyDFS : 根目录
     - dfs : DFS文件夹，用于存放DFS数据
       - name : 存放NameNode数据
         - .log : 存放日志文件（主要是节点宕机日志）。本地文件（不进行备份。
       - data : 存放DataNode数据
         - .code : 存放用户提交的MapReduce代码文件，以job_id进行区分。本地文件（不进行备份）。
         - .res : 存放Map和Reduce的中间结果。本地文件（不进行备份）。
     - common.py  : 全局变量
     - name_node.py : NameNode程序
     - data_node.py : DataNode程序
     - client.py : Client程序，用于用户与DFS交互

3. 模块功能

   * name_node.py
     * 基本功能：
       * 保存文件的块存放位置信息
       * 获取文件/目录信息
       * 倾听data_node心跳，并处理宕机事故
       * 分配map和reduce任务
     * 具体函数：
       - run：创建count_beats线程；运行listen函数；
       - listen：监听连接，多线程调用handle函数对连接请求进行处理；
       - handle：根据请求命令处理连接；
       - heart_beats：接收data_node心跳信号；判断是否有损坏节点重新连接；
       - count_beats：计算data_node心跳信号停止数目，若超过特定时长则对该data_node创建check线程，并停止对该宕机节点计数；
       - check：迭代寻找涉及宕机节点的数据，调用rebuild函数重建备份，并记录宕机日志；
       - rebuild：重建备份；
       - ls：获取文件夹的文件信息或文件的FAT表信息；
       - get_fat_item： 获取文件的FAT表项
       - new_fat_item： 根据文件大小创建FAT表项
       - rm_fat_item： 删除一个FAT表项
       - format: 删除所有FAT表项
       - map_reduce：对用户提交的MapReduce请求建立job_id，分配mapper和reducer；
       - map_worker：向对应的mapper分发map任务；
       - reduce_worker：向对应的reducer分发reduce任务。
   * data_node.py
     * 基本功能：
       * 发送心跳信号，确认存活状态
       * 数据存储、获取与发送
       * map和reduce计算和存储
     * 具体函数：
       * run：创建heart_beats线程，运行listen函数；
       * listen：监听连接，多线程调用handle函数对连接请求进行处理；
       * handle：根据请求命令处理连接；
       * heart_beats：向name_node发送心跳信号；
       * load：加载数据块
       * store：保存数据块
       * send：向特定节点发送数据
       * get：向特定节点请求数据
       * rm：删除数据块
       * format：删除所有数据块
       * map：收集数据，调用用户提交的map函数，执行计算；
       * reduce：收集map结果数据，调用用户提交的reduce函数，执行计算；
   * client.py
     * 基本功能：
       * 提交用户需求，返回对应数据或结果；
     * 具体函数：
       * ls：查看当前目录文件/目录信息；
       * copyFromLocal：从本地复制数据到DFS；
       * sendToHost：发送数据到对应的节点；
       * copyToLocal：从DFS复制数据到本地；
       * rm：删除DFS上的文件；
       * format：格式化DFS；
       * mapReduce：提交MapReduce需求；

#### 3.2 系统细节：

1. 数据分割、存储与运算：

   * 该DFS在数据存储时均匀划分数据，即每个block大小为64MB，block数目为$\biggl\lceil\frac{size_f}{size_b}\biggr\rceil$，其中$size_f$为文件大小，$size_b$为设定的block大小。

   * 在进行数据存储时，先由name_node处获得数据存储的FAT表，其中每个block的各个备份存储的data_node随机选择（同一block的两个或多个备份不会同时存在于一个data_node上）；再由client向data_node发送对应的数据，如下图所示（图片来源：https://www.cnblogs.com/zhangyinhua/p/7681059.html），对每个block，client将其传输到第一个data_node上，在由data_node调用send函数发送到下一个data_node，当三个备份均完成时，依次返回，并最终返回给client确认数据存储完毕。client获取数据时，对每个block的三个备份中，随机选择一个进行数据读取。

     <img src="/Users/mac/Library/Application Support/typora-user-images/image-20191125010438679.png" alt="image-20191125010438679" style="zoom:50%;" />

   * 对数据进行Map运算时，需要找到有效的数据单位（如一行）。参考hadoop的block和split机制，如图（图片来源：https://cloud.tencent.com/developer/article/1481807），若该block非第0个，则从其第二个有效数据单位（第二行）开始进行数据读取；若该block非最后一个，则从其下一个block中获取第一个有效数据单位（第一行）拼接到该block数据的最后进行一起处理，从而避免物理分割过程中对数据逻辑性的破坏。

     <img src="/Users/mac/Library/Application Support/typora-user-images/image-20191125005751604.png" alt="image-20191125005751604" style="zoom:50%;" />

2. Map和Reduce任务分配与整合：

   * 当用户提交一个MapReduce任务时，name_node创建一个job_id，作为该任务的编号；由name_node基于所要处理的数据的FAT表确认map和reduce任务的分配。

   * 该DFS默认依据数据的block数目确认mapper数目，即每个mapper基于一个block进行数据计算，并给定该mapper一个mapper_id；mapper运行所在的data_node与对应block存储的data_node相同。
   * 该DFS默认设置一个reducer，并给定该reducer一个reducer_id；reducer运行所在的data_node随机选择。
   * 在Map过程中，依据用户的map函数，每个mapper将对应block的数据按 <行号，行内容> 的方式整理为 <k, v> 结构的数据（使用DataFrame实现），作为map函数的输入；map函数的输出应当也为 <k, v> 结构的数据（DataFrame结构）；最后，mapper依据job_id和mapper_id存储map的结果数据到指定目录（MyDFS/dfs/data/.res/，本地磁盘，即不进行数据备份）。
   * 在Reduce过程中，每个reducer由每个mapper的信息找到其结果数据存储位置，并获取该数据；由于map的结果为 <k, v> 结构数据，因此可以由多个mapper的结果得到用于reduce输入的 <k, v> 结构数据，并依据用户的reduce函数进行处理。
   * 由于默认设置为一个Reduce，可以直接得到整合后的输出结果。

3. 错误处理：

   * 该DFS实现了节点宕机与恢复的检测，备份重建功能。

   * 由data_node周期性向name_node发送心跳信号，如下图（图片来源：https://www.cnblogs.com/zhangyinhua/p/7681146.html）；

     <img src="/Users/mac/Library/Application Support/typora-user-images/image-20191125010248578.png" alt="image-20191125010248578" style="zoom:50%;" />

   * name_node检测心跳信号，并将持续一定时间长度未发送信号的data_node判断为故障。此时，name_node迭代检查目录树，若某数据的特定block涉及该data_node，则进行记录。之后，对所有记录的位置，利用正常的备份节点进行备份重建，重建位置在正常的、未备份该block的节点中随机选择（注意：若此时正常的节点数目小于默认的备份数目时，则会报错）。重建成功后更新FAT表。

   * 若节点恢复（重新检测到心跳信号），则格式化该data_node的全部数据，以保证系统准确性，并将该节点重新标记为正常。

   * 数据校对（思路，未完成）：data_node周期性向name_node发送状态信息和目录树，name_node迭代对比目录树正确性和各文件大小。

####3.3 实验：

1. 预先写入```.bashrc```的快捷方式：

   ```shell
   2019211309@thumm01:~$ cat .bashrc
   alias pssh="parallel-ssh -h /home/dsjxtjc/hosts"
   alias pscp="parallel-scp -h /home/dsjxtjc/hosts"
   alias pnuke="parallel-nuke -h /home/dsjxtjc/hosts"
   alias pslurp="parallel-slurp -h /home/dsjxtjc/hosts"
   alias my_dfs="python3 ~/MyDFS/client.py"
   alias n_start="python3 ~/MyDFS/name_node.py"
   alias d_start="~/HW2/run_data_node.sh"
   alias d_end="~/HW2/stop_data_node.sh"
   alias re_dfs="~/HW2/depatch_code.sh"
   ```

   设置登录自动更新```source .bashrc```：

   ```shell
   2019211309@thumm01:~$ cat .bash_profile
   if test -f .bashrc ; then
   source .bashrc
   fi
   ```

   其中涉及三个脚本文件分别是：

   * ```run_data_node.sh```：用于在各集群上运行data_node

     ```shell
     #!/bin/bash --login
     pssh "python3 ~/MyDFS/data_node.py"
     ```

   * ```stop_data_node.sh```：用于终止各集群上运行的data_node

     ```shell
     #!/bin/bash --login
     for ((i=1;i<=5;i++));do
     	ssh thumm0$i "pkill -f 'python3 /home/dsjxtjc/2019211309/MyDFS/data_node.py'"
     done
     ```

   * ```detach_code.sh```：用于更新各集群上的代码，只更新```data_node.py```和```common.py```

     ```shell
     #!/bin/bash --login
     pssh "mkdir -p ~/MyDFS/"
     for ((i=1;i<=5;i++));do
     	scp ~/MyDFS/data_node.py ~/MyDFS/common.py thumm0$i:~/MyDFS/
     done
     ```

2. 代码更新以及系统启动：

   * 本地代码上传：

     ```shell
     $ scp -r ./MyDFS 2019211309@219.223.181.246:~/
     ```

   * 集群代码更新（重启各个data_node节点生效）：

     ```shell
     $ re_dfs
     ```
     
     ![image-20191125011304323](/Users/mac/Library/Application Support/typora-user-images/image-20191125011304323.png)
     
   * 启动name_node：
   
     ```shell
     $ n_start
     ```
   
     ![image-20191125011231185](/Users/mac/Library/Application Support/typora-user-images/image-20191125011231185.png)
   
   * 启动/终止data_node：
   
     ```shell
     $ d_start
     $ d_end
     ```
3. 数据实验：

   **数据概述：**

   使用python生成符合分布$N(0,1)$的数据1,000,000个，并复制76份得到1.9G的数据文件。相同数据的重复不会改变其均值和方差。

   ```python
   >>> import numpy as np
   >>> data = np.random.randn(1000000)
   >>> np.savetxt('numbers_normal.txt',data)
   >>> print("mean: ", np.mean(data), "\nvar: ", np.var(data))
   ```

   ![image-20191114221320116](/Users/mac/Library/Application Support/typora-user-images/image-20191114221320116.png)

   数据存储路径：```/home/dsjxtjc/2019211309/data/data.txt```

   ```shell
   $ wc -l ~/data/data.txt
   ```

   ![image-20191125012226787](/Users/mac/Library/Application Support/typora-user-images/image-20191125012226787.png)

   ```shell
   $ ls -lh ~/data/data.txt
   ```

   ![image-20191125012316661](/Users/mac/Library/Application Support/typora-user-images/image-20191125012316661.png)

   * ```copyFromLocal```命令：

     ```shell
     $ my_dfs -copyFromLocal ~/data/data.txt /data/data.txt
     ```

     ![image-20191125012759136](/Users/mac/Library/Application Support/typora-user-images/image-20191125012759136.png)

   * ```ls```命令：

     ```shell
     $ my_dfs -ls /data/
     $ my_dfs -ls /data/data.txt
     ```

     ![image-20191125015507661](/Users/mac/Library/Application Support/typora-user-images/image-20191125015507661.png)

     ......

   * ```copyToLocal```命令：

     ```shell
     $ my_dfs -copyToLocal /data/data.txt ~/data/data2.txt
     ```

     ![image-20191125014657562](/Users/mac/Library/Application Support/typora-user-images/image-20191125014657562.png)

     ......

     ![image-20191125014600848](/Users/mac/Library/Application Support/typora-user-images/image-20191125014600848.png)

     检验数据正确性：

     ```shell
     $ diff ~/data/data.txt ~/data/data2.txt
     ```

     ![image-20191125014924406](/Users/mac/Library/Application Support/typora-user-images/image-20191125014924406.png)

   * ```format```命令：

     ```shell
     $ my_dfs -format
     ```

     ![image-20191125012537514](/Users/mac/Library/Application Support/typora-user-images/image-20191125012537514.png)

   * ```mapReduce```命令（map.py和reduce.py见附件）：

     ```shell
     $ my_dfs -mapReduce ~/MyDFS/map.py ~/MyDFS/reduce.py /data/data.txt
     ```

     ![image-20191125015417055](/Users/mac/Library/Application Support/typora-user-images/image-20191125015417055.png)

     可见，使用MapReduce实现的均值方差计算与数据生成时用python计算的结果一致。

4. 错误检查能力实验：

   * 通过手动关闭一个节点（以thumm02为例），观察name_node的变化。无心跳时间限定为50s。

     ![image-20191125015933437](/Users/mac/Library/Application Support/typora-user-images/image-20191125015933437.png)

     ......

     ![image-20191125015959264](/Users/mac/Library/Application Support/typora-user-images/image-20191125015959264.png)

   * name_node输出了新的FAT表。

     当thumm02重新连入时，name_node将其格式化。

     ![image-20191125020324233](/Users/mac/Library/Application Support/typora-user-images/image-20191125020324233.png)

### 总结：

本次实验实现了DFS的基本功能，加深了对分布式文件系统的理解。在实验的过程中，遇到的主要问题有：socket通信问题、数据分割问题、MapReduce的 <k, v> 结构化接口、数据重建思路等。此外，还有分布式文件系统内部的通信逻辑，如在数据存储时使用client向某block所有需要备份的节点传输数据，还是传输给第一个data_node，再由第一个data_node传输给后续的data_node；MapReduce的任务分配方式和数据汇总方式，如根据block设定mapper还是根据data_node设定mapper，利用job_id和mapper_id等识别文件进行reduce的数据汇总等。hadoop在这些方面的实现思路，使我能够对各种实现方式的利弊有一定的认识。

