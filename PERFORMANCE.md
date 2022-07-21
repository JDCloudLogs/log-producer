# 性能测试

## 测试环境
### 测试机器
* 实例规格：h.g2.large
* CPU：2 vCPU，Intel(R) Xeon(R) Gold 6146 CPU @ 3.20GHz
* MEM：8 GB
* OS：Linux version 3.10.0-327.el7.x86_64

### JDK
java version "1.8.0_144"
Java(TM) SE Runtime Environment (build 1.8.0_144-b01)
Java HotSpot(TM) 64-Bit Server VM (build 25.144-b01, mixed mode)

### 日志样例
测试中使用的日志包含 4 个键值对。为了测试数据的顺序，我们给数据增加了一个 sequence 的前缀，sequence 是从1开始自增的数字。单条日志大小约为 580 字节，格式如下：
``` 
level:  INFO
thread:  pool-1-thread-2
location:  com.jdcloud.logs.producer.core.BatchSender.sendBatch(BatchSender.java:117)
message:  <sequence>This is a test message,测试日志_____abcdefghijklmnopqrstuvwxyz~!@#$%^&*()_0123456789,测试日志_____abcdefghijklmnopqrstuvwxyz~!@#$%^&*()_0123456789,测试日志_____abcdefghijklmnopqrstuvwxyz~!@#$%^&*()_0123456789,测试日志_____abcdefghijklmnopqrstuvwxyz~!@#$%^&*()_0123456789,测试日志_____abcdefghijklmnopqrstuvwxyz~!@#$%^&*()_0123456789,测试日志_____abcdefghijklmnopqrstuvwxyz~!@#$%^&*()_0123456789
```

## 测试用例

### 测试程序说明
* ProducerConfig.totalSizeInBytes: 默认值 104,857,600（即 100 MB），具体用例中调整
* ProducerConfig.batchSizeInBytes: 默认值 2,097,152，具体用例中调整
* ProducerConfig.batchCountThreshold：32768
* ProducerConfig.batchMillis：2000
* ProducerConfig.sendThreads: 默认值可用处理器个数，具体用例中调整
* JVM 初始堆大小：2 GB
* JVM 最大堆大小：2 GB
* 调用`Producer.send()`方法的线程数量：10
* 每个线程发送日志条数：20,000,000
* 发送日志总大小：约 108 GB
* 客户端压缩后大小：约 1.5 GB
* 发送日志总条数：200,000,000

### 调整发送线程数 sendThreads
将日志缓存总大小 totalSizeInBytes 设置为默认值 104,857,600（即 100 MB），每批次发送的日志字节数 batchSizeInBytes 设置为默认值 2,097,152（即 2 MB），通过调整发送线程数 sendThreads 观察程序性能。

| IO 线程数量 | 原始数据吞吐量 | 压缩后数据吞吐量 | CPU 使用率 |
| -------- | -------- | -------- | -------- |
| 1 | 18.497 MB/s | 0.253 MB/s | 45% |
| 2 | 36.583 MB/s | 0.501 MB/s | 115% |
| 4 | 68.309 MB/s | 0.936 MB/s | 160% |
| 6 | 70.576 MB/s | 0.967 MB/s | 170% |
| 8 | 45.158 MB/s | 0.619 MB/s | 160% |

### 调整日志缓存总大小 totalSizeInBytes
将发送线程数 sendThreads 设置为4，每批次发送的日志字节数 batchSizeInBytes 设置为默认值 2,097,152（即 2 MB），通过调整日志缓存总大小 totalSizeInBytes 观察程序性能。

| totalSizeInBytes | 原始数据吞吐量 | 压缩后数据吞吐量 | CPU 使用率 |
| -------- | -------- | -------- | -------- |
| 26,214,400 | 43.834 MB/s | 0.600 MB/s | 150% |
| 52,428,800 | 65.400 MB/s | 0.896 MB/s | 160% |
| 104,857,600 | 68.309 MB/s | 0.936 MB/s | 160% |
| 209,715,200 | 67.352 MB/s | 0.923 MB/s | 160% |

### 调整每批次发送的日志字节数 batchSizeInBytes
将发送线程数 sendThreads 设置为4，日志缓存总大小 totalSizeInBytes 设置为默认值 104,857,600（即 100 MB），通过调整每批次发送的日志字节数 batchSizeInBytes 观察程序性能。

| batchSizeInBytes | 原始数据吞吐量 | 压缩后数据吞吐量 | CPU 使用率 | 请求日志服务压缩后大小 | 请求日志服务响应时间 |
| -------- | -------- | -------- | -------- | -------- | -------- |
| 524,288 | 61.991 MB/s | 0.849 MB/s | 100% | 8k | 25ms |
| 1,048,576 | 57.902 MB/s | 0.793 MB/s | 110% | 16k | 60ms |
| 2,097,152 | 68.309 MB/s | 0.936 MB/s | 160% | 32k | 110ms |
| 4,194,304 | 18.710 MB/s | 0.256 MB/s | 140% | 64k | 220ms |

## 总结
1. 增加发送线程数量可以显著提高吞吐量，尤其是当发送线程数少于可用处理器个数时，发送线程数为默认值处理器个数时比较合适。
2. 调整日志缓存总大小对吞吐量影响不明显，建议使用默认值100MB。
3. 调整每批次发送字节数对吞吐量有一定影响，默认值2MB左右比较合适。