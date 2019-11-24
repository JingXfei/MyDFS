#!/bin/bash --login
for ((i=1;i<=5;i++));do
	ssh thumm0$i "pkill -f 'python3 /home/dsjxtjc/2019211309/MyDFS/data_node.py'"
done
