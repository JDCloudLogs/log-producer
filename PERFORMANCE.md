# 性能测试

## 测试环境
### 测试机器
* 规格：c.n3.xlarge（4核8GB 计算优化 标准型)
* CPU：4 CPU，Intel(R) Xeon(R) Platinum 8338C CPU @ 2.60GHz
* MEM：8 GB
* 内网带宽：1 Mbps
* OS：Linux version 3.10.0-693.el7.x86_64

### JDK
java version "1.8.0_181"
Java(TM) SE Runtime Environment (build 1.8.0_181-b13)
Java HotSpot(TM) 64-Bit Server VM (build 25.181-b13, mixed mode)

### 日志样例
测试中使用的日志包含 4 个键值对。为了测试数据的随机性，防止数据压缩比太高，我们给数据增加了一个 sequence 的后缀，sequence 取值范围是 0~单线程发送次数。单条日志大小约为 580 字节，格式如下：
``` 
level:  INFO_<sequence>
thread:  pool-1-thread-2_<sequence>
location:  com.jdcloud.logs.producer.core.BatchSender.sendBatch(BatchSender.java:117)_<sequence>
message:  0测试日志_____abcdefghijklmnopqrstuvwxyz~!@#$%^&*()_0123456789_<sequence>,1测试日志_____abcdefghijklmnopqrstuvwxyz~!@#$%^&*()_0123456789_<sequence>,2测试日志_____abcdefghijklmnopqrstuvwxyz~!@#$%^&*()_0123456789_<sequence>,3测试日志_____abcdefghijklmnopqrstuvwxyz~!@#$%^&*()_0123456789_<sequence>,4测试日志_____abcdefghijklmnopqrstuvwxyz~!@#$%^&*()_0123456789_<sequence>,5测试日志_____abcdefghijklmnopqrstuvwxyz~!@#$%^&*()_0123456789_<sequence>
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
* 发送日志总大小：约 120 GB
* 客户端压缩后大小：约 13.3 GB
* 发送日志总条数：200,000,000

### 调整发送线程数 sendThreads
将日志缓存总大小 totalSizeInBytes 设置为默认值 104,857,600（即 100 MB），每批次发送的日志字节数 batchSizeInBytes 设置为默认值 2,097,152（即 2 MB），通过调整发送线程数 sendThreads 观察程序性能。

| IO 线程数量 | 原始数据吞吐量 | 压缩后数据吞吐量 | CPU 使用率 |
| -------- | -------- | -------- | -------- |
| 1 | 16.391 MB/s | 1.821 MB/s | 23% |
| 2 | 33.616 MB/s | 3.735 MB/s | 48% |
| 4 | 66.123 MB/s | 7.347 MB/s | 101% |
| 8 | 127.386 MB/s | 14.154 MB/s | 166% |
| 16 | 205.347 MB/s | 22.816 MB/s | 244% |
| 32 | 298.335 MB/s | 33.148 MB/s | 355% |

### 调整日志缓存总大小 totalSizeInBytes
将发送线程数 sendThreads 设置为16，每批次发送的日志字节数 batchSizeInBytes 设置为默认值 2,097,152（即 2 MB），通过调整日志缓存总大小 totalSizeInBytes 观察程序性能。

| totalSizeInBytes | 原始数据吞吐量 | 压缩后数据吞吐量 | CPU 使用率 |
| -------- | -------- | -------- | -------- |
| 26,214,400 | 99.732 MB/s | 11.081 MB/s | 82% |
| 52,428,800 | 191.779 MB/s | 21.309 MB/s | 188% |
| 104,857,600 | 205.347 MB/s | 22.816 MB/s | 244% |
| 209,715,200 | 170.373 MB/s | 18.930 MB/s | 265% |

### 调整每批次发送的日志字节数 batchSizeInBytes
将发送线程数 sendThreads 设置为16，日志缓存总大小 totalSizeInBytes 设置为默认值 104,857,600（即 100 MB），通过调整每批次发送的日志字节数 batchSizeInBytes 观察程序性能。

| batchSizeInBytes | 原始数据吞吐量 | 压缩后数据吞吐量 | CPU 使用率 | 请求日志服务压缩后大小(KB) | 请求日志服务响应时间(ms) |
| -------- | -------- | -------- | -------- | -------- | -------- |
| 524,288 | 169.785 MB/s | 17.932 MB/s | 237 % | 582（每批次大小差异不大） | avg=48 min=18 max=7113 |
| 1,048,576 | 184.886 MB/s | 19.486 MB/s | 242 % | 1163（每批次大小差异不大） | avg=88 min=14 max=4178 |
| 2,097,152 | 205.347 MB/s | 22.816 MB/s | 244% | 2328（每批次大小差异不大） | avg=162 min=27 max=6581 |
| 4,194,304 | 193.802 MB/s | 20.396 MB/s | 263% | 4655（每批次大小差异不大） | avg=295 min=142 max=5994 |

## 总结
1. 增加发送线程数量可以显著提高吞吐量，尤其是当发送线程数少于 可用处理器个数 * 2 时。
2. 调整日志缓存总大小对吞吐量影响不明显，建议使用默认值100MB。
3. 调整每批次发送字节数对吞吐量影响不明显，默认值2MB左右比较合适。