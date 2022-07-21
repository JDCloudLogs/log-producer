package com.jdcloud.logs.producer;

import com.jdcloud.logs.api.common.LogContent;
import com.jdcloud.logs.api.common.LogItem;
import com.jdcloud.logs.producer.config.ProducerConfig;
import com.jdcloud.logs.producer.config.RegionConfig;
import com.jdcloud.logs.producer.errors.ProducerException;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * log producer test
 *
 * @author liubai
 * @date 2022/7/12
 */
public class LogProducerTest {

    public final static String logTopic = System.getProperty("logTopic");
    public final static String accessKeyId = System.getProperty("accessKeyId");
    public final static String secretAccessKey = System.getProperty("secretAccessKey");
    public final static String regionId = System.getProperty("regionId");
    public final static String endpoint = System.getProperty("endpoint");

    /**
     * 冒烟测试
     */
    @Test
    public void send() throws ProducerException, InterruptedException {
        Producer producer = getProducer();

        List<LogItem> logItems = new ArrayList<LogItem>();
        logItems.add(buildLogItem(0));
        logItems.add(buildLogItem(1));

        producer.send(regionId, logTopic, logItems);

        producer.close();
        assertProducerFinalState(producer);
    }

    /**
     * 测试一次发送多组日志
     */
    @Test
    public void sendGroup() throws ProducerException, InterruptedException {
        Producer producer = getProducer();
        for (int i = 0; i < 100; i++) {
            List<LogItem> logItems = new ArrayList<LogItem>();
            logItems.add(buildLogItem(0));
            logItems.add(buildLogItem(1));

            producer.send(regionId, logTopic, logItems);
        }

        producer.close();
        assertProducerFinalState(producer);
    }

    /**
     * 测试按时间间隔发送日志
     */
    @Test
    public void sendTimeout() throws ProducerException, InterruptedException {
        Producer producer = getProducer();
        for (int i = 0; i < 100; i++) {
            List<LogItem> logItems = new ArrayList<LogItem>();
            logItems.add(buildLogItem(0));
            logItems.add(buildLogItem(1));

            Thread.sleep(500);
            producer.send(regionId, logTopic, logItems);
        }

        producer.close();
        assertProducerFinalState(producer);
    }

    /**
     * 测试日志收敛
     * <p>
     * {@link ProducerConfig#isLogConvergence()}
     */
    @Test
    public void logConvergence() throws ProducerException, InterruptedException {
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setLogConvergence(true);
        producerConfig.setLogConvergenceMillis(5000);
        RegionConfig regionConfig = new RegionConfig(accessKeyId, secretAccessKey, regionId, endpoint);
        Producer producer = new LogProducer(producerConfig);
        producer.putRegionConfig(regionConfig);

        for (int i = 0; i < 100; i++) {
            List<LogItem> logItems = new ArrayList<LogItem>();
            logItems.add(buildLogItem(0));
            logItems.add(buildLogItem(1));

            Thread.sleep(500);
            producer.send(regionId, logTopic, logItems);
        }

        producer.close();
        assertProducerFinalState(producer);
    }

    /**
     * 发送大批量日志
     */
    @Test
    public void bulkLogs() throws ProducerException, InterruptedException {
        ProducerConfig producerConfig = new ProducerConfig();
        RegionConfig regionConfig = new RegionConfig(accessKeyId, secretAccessKey, regionId, endpoint);
        final Producer producer = new LogProducer(producerConfig);
        producer.putRegionConfig(regionConfig);

        final int tasks = 100;
        final int times = 10000;
        final AtomicInteger logId = new AtomicInteger(0);
        ExecutorService executorService = Executors.newFixedThreadPool(6);
        final CountDownLatch latch = new CountDownLatch(tasks);
        for (int i = 0; i < tasks; ++i) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (int i = 0; i < times; ++i) {
                            producer.send(regionId, logTopic, buildLogItem(logId.getAndIncrement()));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    latch.countDown();
                }
            });
        }
        latch.await();
        executorService.shutdown();
        Thread.sleep(producerConfig.getBatchMillis() * 2L);
        producer.close();
        assertProducerFinalState(producer);
    }

    /**
     * 发送大批量日志
     */
    @Test
    public void bulkBatchLogs() throws ProducerException, InterruptedException {
        ProducerConfig producerConfig = new ProducerConfig();
        RegionConfig regionConfig = new RegionConfig(accessKeyId, secretAccessKey, regionId, endpoint);
        final Producer producer = new LogProducer(producerConfig);
        producer.putRegionConfig(regionConfig);

        final int tasks = 100;
        final int times = 1000;
        final AtomicInteger logId = new AtomicInteger(0);
        ExecutorService executorService = Executors.newFixedThreadPool(6);
        final CountDownLatch latch = new CountDownLatch(tasks);
        for (int i = 0; i < tasks; ++i) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (int i = 0; i < times; ++i) {
                            producer.send(regionId, logTopic, buildLogItems(50, logId));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    latch.countDown();
                }
            });
        }
        latch.await();
        executorService.shutdown();
        Thread.sleep(producerConfig.getBatchMillis() * 2L);
        producer.close();
        assertProducerFinalState(producer);
    }

    /**
     * 失败重试测试
     */
    @Test
    public void retry() throws ProducerException, InterruptedException {
        Producer producer = getProducer();

        List<LogItem> logItems = new ArrayList<LogItem>();
        logItems.add(buildLogItem(0));
        logItems.add(buildLogItem(1));

        producer.send(regionId, logTopic, logItems);

        Thread.sleep(30000);

        producer.close();

        assertProducerFinalState(producer);
    }

    /**
     * 测试日志大小
     */
    @Test
    public void logSizeInBytes() throws ProducerException, InterruptedException {
        System.err.println("Single log sizeInBytes=" + calculate(buildLogItem(0)));

        int produceTimes = 5000;
        Producer producer = getProducer();
        for (int i = 0; i < produceTimes; i++) {
            List<LogItem> logItems = new ArrayList<LogItem>();
            logItems.add(buildLogItem(0));
            logItems.add(buildLogItem(1));

            producer.send(regionId, logTopic, logItems);
        }

        producer.close();

        assertProducerFinalState(producer);
    }

    private Producer getProducer() {
        ProducerConfig producerConfig = new ProducerConfig();
        RegionConfig regionConfig = new RegionConfig(accessKeyId, secretAccessKey, regionId, endpoint);
        Producer producer = new LogProducer(producerConfig);
        producer.putRegionConfig(regionConfig);
        return producer;
    }

    public static LogItem buildLogItem(int seq) {
        LogItem logItem = new LogItem(System.currentTimeMillis());
        logItem.addContent("level", "INFO");
        logItem.addContent("thread", "pool-1-thread-2");
        logItem.addContent("location", "com.jdcloud.logs.producer.core.BatchSender.sendBatch(BatchSender.java:117)");
        logItem.addContent("message", seq + "This is a test message,"
                + "测试日志_____abcdefghijklmnopqrstuvwxyz~!@#$%^&*()_0123456789,"
                + "测试日志_____abcdefghijklmnopqrstuvwxyz~!@#$%^&*()_0123456789,"
                + "测试日志_____abcdefghijklmnopqrstuvwxyz~!@#$%^&*()_0123456789,"
                + "测试日志_____abcdefghijklmnopqrstuvwxyz~!@#$%^&*()_0123456789,"
                + "测试日志_____abcdefghijklmnopqrstuvwxyz~!@#$%^&*()_0123456789,"
                + "测试日志_____abcdefghijklmnopqrstuvwxyz~!@#$%^&*()_0123456789");
//        logItem.addContent("message", "这是一条测试日志这是一条测试日志这是一条测试日志这是一条测试日志这是一条测试日志"
//                +"这是一条测试日志这是一条测试日志这是一条测试日志这是一条测试日志这是一条测试日志"
//                +"这是一条测试日志这是一条测试日志这是一条测试日志这是一条测试日志这是一条测试日志"
//                +"这是一条测试日志这是一条测试日志这是一条测试日志这是一条测试日志这是一条测试日志"
//                +"这是一条测试日志这是一条测试日志这是一条测试日志这是一条测试日志这是一条测试日志"
//                +"这是一条测试日志这是一条测试日志这是一条测试日志这是一条测试日志这是一条测试日志"
//                +"这是一条测试日志这是一条测试日志这是一条测试日志这是一条测试日志这是一条测试日志"
//                + seq);
        return logItem;
    }

    public static List<LogItem> buildLogItems(int size, AtomicInteger logId) {
        List<LogItem> logItems = new ArrayList<LogItem>();
        for (int i = 0; i < size; i++) {
            logItems.add(buildLogItem(logId.getAndIncrement()));
        }
        return logItems;
    }

    public static int calculate(LogItem logItem) {
        int sizeInBytes = 8;
        for (LogContent content : logItem.getContents()) {
            if (content.getKey() != null) {
                sizeInBytes += content.getKey().length();
            }
            if (content.getValue() != null) {
                sizeInBytes += content.getValue().length();
            }
        }
        return sizeInBytes;
    }

    public static void assertProducerFinalState(Producer producer) {
        Assert.assertEquals(0, producer.getLogCount());
        Assert.assertEquals(producer.getProducerConfig().getTotalSizeInBytes(), producer.availableMemoryInBytes());
    }
}


