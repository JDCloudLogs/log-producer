# JDCloud log producer Java SDK

JDCloud log producer Java SDK 是一个用于大数据量、高并发场景下发送日志的java类库。


## 安装

### Maven配置
将下列依赖加入到您项目的 pom.xml 中。
```
<dependency>
    <groupId>com.jdcloud.logs</groupId>
    <artifactId>log-producer</artifactId>
    <version>0.2.0</version>
</dependency>
```


## 配置
| 名称 | 类型 | 必填 | 默认值 | 说明 |
| -------- | -------- | -------- | -------- | -------- |
| accessKeyId | String | 是 |  | 认证标识 |
| secretAccessKey | String | 是 |  | 认证秘钥 |
| regionId | String | 是 |  | 地域标识，可选值：cn-north-1、cn-south-1、cn-east-1、cn-east-2 |
| endpoint | String | 是 |  | 日志发送地址，可选值：logs.internal.cn-north-1.jdcloud-api.com、logs.internal.cn-south-1.jdcloud-api.com、logs.internal.cn-east-1.jdcloud-api.com、logs.internal.cn-east-2.jdcloud-api.com |
| totalSizeInBytes | int | 否 | 100 * 1024 * 1024 | 日志缓存的内存占用字节数上限，默认为100M |
| maxBlockMillis | long | 否 | 0 | 日志缓存达到上限后，获取可用内存的最大阻塞等待时间，默认为0，表示不等待。等待超时后丢弃日志，由于会阻塞日志线程，建议设置为0 |
| sendThreads | int | 否 | 可用处理器个数 | 日志发送线程数，默认为可用处理器个数 |
| batchSize | int | 否 | 4096 | 每批次发送的日志数，默认为4096，最大不能超过32768 |
| batchSizeInBytes | int | 否 | 2 * 1024 * 1024 | 每批次发送的日志字节数，默认2MB，最大不能超过4MB |
| batchMillis | int | 否 | 2000 | 批次发送时间间隔毫秒数，默认为2秒，表示每隔2秒发送一个批次日志，最小不少于100毫秒 |
| retries | int | 否 | 10 | 发送失败后重试次数，0为不重试，默认重试10次 |
| initRetryBackoffMillis | long | 否 | 100 | 发送失败后首次重试时间毫秒数 |
| maxRetryBackoffMillis | long | 否 | 50 * 1000 | 发送失败后最大退避时间毫秒数，默认为50秒 |
