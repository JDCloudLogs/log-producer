# 性能测试

## 测试环境
### 测试机器
* CPU：6 CPU，2.6 GHz 六核Intel Core i7
* MEM：16 GB
* 内网带宽：5 Gbps
* OS：macOS Catalina 10.15.7

### JDK
java version "1.8.0_181"
Java(TM) SE Runtime Environment (build 1.8.0_181-b13)
Java HotSpot(TM) 64-Bit Server VM (build 25.181-b13, mixed mode)

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
* JVM 初始堆大小：4 GB
* JVM 最大堆大小：4 GB
* 调用`Producer.send()`方法的线程数量：10
* 每个线程发送日志条数：20,000,000
* 发送日志总大小：约 108 GB
* 客户端压缩后大小：约 1.5 GB
* 发送日志总条数：200,000,000

### 调整发送线程数 sendThreads
将日志缓存总大小 totalSizeInBytes 设置为默认值 104,857,600（即 100 MB），每批次发送的日志字节数 batchSizeInBytes 设置为默认值 2,097,152（即 2 MB），通过调整发送线程数 sendThreads 观察程序性能。

| IO 线程数量 | 原始数据吞吐量 | 压缩后数据吞吐量 | CPU 使用率 |
| -------- | -------- | -------- | -------- |
| 1 | 17.834 MB/s | 0.244 MB/s | 50% |
| 2 | 35.155 MB/s | 0.440 MB/s | 105% |
| 4 | 52.562 MB/s | 0.720 MB/s | 540% |
| 6 | 50.308 MB/s | 0.689 MB/s | 580% |
| 10 | 35.881 MB/s | 0.492 MB/s | 540% |

### 调整日志缓存总大小 totalSizeInBytes
将发送线程数 sendThreads 设置为4，每批次发送的日志字节数 batchSizeInBytes 设置为默认值 2,097,152（即 2 MB），通过调整日志缓存总大小 totalSizeInBytes 观察程序性能。

| totalSizeInBytes | 原始数据吞吐量 | 压缩后数据吞吐量 | CPU 使用率 |
| -------- | -------- | -------- | -------- |
| 26,214,400 | 52.366 MB/s | 0.717 MB/s | 550% |
| 52,428,800 | 56.320 MB/s | 0.772 MB/s | 540% |
| 104,857,600 | 52.562 MB/s | 0.720 MB/s | 540% |
| 209,715,200 | 40.93 MB/s | 0.561 MB/s | 560% |

### 调整每批次发送的日志字节数 batchSizeInBytes
将发送线程数 sendThreads 设置为4，日志缓存总大小 totalSizeInBytes 设置为默认值 104,857,600（即 100 MB），通过调整每批次发送的日志字节数 batchSizeInBytes 观察程序性能。

| batchSizeInBytes | 原始数据吞吐量 | 压缩后数据吞吐量 | CPU 使用率 | 请求日志服务压缩后大小 | 请求日志服务响应时间 |
| -------- | -------- | -------- | -------- | -------- | -------- |
| 524,288 | 42.235 MB/s | 0.579 MB/s | 90 % | 8k | 50ms |
| 1,048,576 | 53.308 MB/s | 0.730 MB/s | 120 % | 16k | 70ms |
| 2,097,152 | 56.320 MB/s | 0.772 MB/s | 540% | 32k | 110ms |
| 4,194,304 | 24.027 MB/s | 0.329 MB/s | 550% | 64k | 250ms |

## 总结
1. 增加发送线程数量可以显著提高吞吐量，尤其是当发送线程数少于可用处理器个数时，发送线程数为默认值处理器个数时比较合适。
2. 调整日志缓存总大小对吞吐量影响不明显，建议使用默认值100MB。
3. 调整每批次发送字节数对吞吐量有一定影响，默认值2MB左右比较合适。