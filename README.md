# JDCloud log producer Java SDK

JDCloud log producer Java SDK 是一个用于大数据量、高并发场景下发送日志的java类库。


## 安装

### Maven配置
将下列依赖加入到您项目的 pom.xml 中。
```
<dependency>
    <groupId>com.jdcloud.logs</groupId>
    <artifactId>log-producer</artifactId>
    <version>0.2.4</version>
</dependency>
```


## 配置
| 名称 | 类型 | 必填 | 默认值               | 说明                                                                                                                                                                           |
| -------- | -------- | -------- |-------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| accessKeyId | String | 是 |                   | 认证标识                                                                                                                                                                         |
| secretAccessKey | String | 是 |                   | 认证秘钥                                                                                                                                                                         |
| regionId | String | 是 |                   | 地域标识，可选值：cn-north-1、cn-south-1、cn-east-1、cn-east-2                                                                                                                           |
| endpoint | String | 是 |                   | 日志发送地址，可选值：logs.internal.cn-north-1.jdcloud-api.com、logs.internal.cn-south-1.jdcloud-api.com、logs.internal.cn-east-1.jdcloud-api.com、logs.internal.cn-east-2.jdcloud-api.com |
| totalSizeInBytes | int | 否 | 100 * 1024 * 1024 | 日志缓存的内存占用字节数上限，默认为100M。且在 ignoreInterruptOnSend=true 时，会在构造时按上限基础扩容5%                                                                                                        |
| maxBlockMillis | long | 否 | 0                 | 日志缓存达到上限后，获取可用内存的最大阻塞等待时间，默认为0毫秒。等待超时后丢弃日志，由于会阻塞日志线程，建议根据实际场景设置                                                                                                              |
| sendThreads | int | 否 | 可用处理器个数           | 日志发送线程数，默认为可用处理器个数                                                                                                                                                           |
| batchSize | int | 否 | 4096              | 每批次发送的日志数，默认为4096，最大不能超过32768                                                                                                                                                |
| batchSizeInBytes | int | 否 | 2 * 1024 * 1024   | 每批次发送的日志字节数，默认2MB，最大不能超过4MB                                                                                                                                                  |
| batchMillis | int | 否 | 2000              | 批次发送时间间隔毫秒数，默认为2秒，表示每隔2秒发送一个批次日志，最小不少于100毫秒                                                                                                                                  |
| retries | int | 否 | 3                 | 发送失败后重试次数，0为不重试，默认重试3次                                                                                                                                                       |
| initRetryBackoffMillis | long | 否 | 100               | 发送失败后首次重试时间毫秒数                                                                                                                                                               |
| maxRetryBackoffMillis | long | 否 | 50 * 1000         | 发送失败后最大退避时间毫秒数，默认为50秒                                                                                                                                                        |
| ignoreInterruptOnSend | boolean | 否 | true              | 当线程中断时是否忽略中断继续发送日志。true: 忽略中断，尝试非阻塞获取资源；若最终未获得资源则直接失败以保障稳定性；false: 抛出 InterruptedException，由调用方处理。默认为 true                                                                   |
| interruptSendTimeoutMillis | long | 否 | 100               | 线程中断时非阻塞获取资源的最大等待时间（毫秒），仅当 ignoreInterruptOnSend 为 true 时生效，默认为100毫秒                                                                                                         |
| sendQueueCapacity | int | 否 | 8192              | 发送线程池队列容量（有界），满载时触发反压（CallerRuns），避免无界堆积                                                                                                                                     |
| retryQueueCapacity | int | 否 | 8192              | 重试队列容量（有界），满载时直接失败并走失败回调，避免堆积导致内存膨胀                                                                                                                                          |
| responseQueueCapacity | int | 否 | 8192              | 成功/失败响应队列容量（有界），用于限制响应堆积形成反压                                                                                                                                                 |
 
### 稳定性与反压说明
- ignoreInterruptOnSend=true 时，ResourceHolder 在构造阶段按 totalSizeInBytes 基础扩容 5%，提升在中断场景下的可用缓冲，一般在类似XXL JOB等定时任务框架在主JOB线程结束后依然需要发送的场景下配置。
- 在忽略中断模式下仍会尝试非阻塞获取资源；若在 interruptSendTimeoutMillis 内无法获得资源，则直接失败返回，不发布到 Disruptor，避免下游队列堆积导致 OOM。
- 发送线程池队列、重试队列、响应队列均为有界，分别受 sendQueueCapacity、retryQueueCapacity、responseQueueCapacity 控制，形成端到端反压闭环。
