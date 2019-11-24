#!/bin/bash --login
for ((i=1;i<=5;i++));do
	scp ~/MyDFS/data_node.py ~/MyDFS/common.py thumm0$i:~/MyDFS/
done
