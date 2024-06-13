# 6.5840
## Lab1
- 每个文件对应一个map任务，**每个**map将处理得到的键值对根据`ihash(kv.Key) % reply.NReduce`保存在`NReduce`个文件中。  
- 超时则释放任务  

## Lab2
- ~~TestMemManyAppends 没过，TODO~~  
- ~~前面的test理解可能也存在错误~~  
- req 里由保存完整的 value 改为保存data里value的尾下标  
