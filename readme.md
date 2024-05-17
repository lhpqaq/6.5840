# 6.5840
## Lab1
- 每个文件对应一个map任务，**每个**map将处理得到的键值对根据`ihash(kv.Key) % reply.NReduce`保存在`NReduce`个文件中。
- 超时则释放任务