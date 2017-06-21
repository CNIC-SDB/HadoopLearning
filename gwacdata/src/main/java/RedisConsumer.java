import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.*;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by liuang on 2017/1/19.
 */
public class RedisConsumer {
    static volatile Boolean isDone = false;

    public static void main(String[] args) throws IOException, InterruptedException {
        long startTime = new Date().getTime();
        final ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();
        queue.offer("开始：" + new Date().toString());
        AtomicInteger count = new AtomicInteger(0);
        int readThreadNum = Integer.valueOf(args[0]);
        CountDownLatch latch = new CountDownLatch(readThreadNum);
        ExecutorService pool = Executors.newFixedThreadPool(readThreadNum);
        Set<HostAndPort> jedisClusterNodes = new HashSet<>();
        for (int start = 68, port = 7001; start < 88; start++) {
            for (int i = 0; i < 4; i++, port++) {
                jedisClusterNodes.add(new HostAndPort("10.0.83." + start, port));
            }
        }
        JedisCluster js = new JedisCluster(jedisClusterNodes);

        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource("/home/wamdm/hbase-1.2.4/conf/hbase-site.xml");
        configuration.addResource("/home/wamdm/hadoop-2.7.3/etc/hadoop/core-site.xml");
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf("gwacdata3600"));
        params.writeBufferSize(Long.valueOf(args[1]))
                .pool(Executors.newFixedThreadPool(Integer.valueOf(args[2])))
                .listener(new BufferedMutator.ExceptionListener() {
                    @Override
                    public void onException(RetriesExhaustedWithDetailsException exception, BufferedMutator mutator) throws RetriesExhaustedWithDetailsException {
                        String ex = exception.getLocalizedMessage();
                        queue.offer(ex);
                        for (StackTraceElement stackTraceElement : exception.getStackTrace()) {
                            queue.offer("\tat " + stackTraceElement);
                        }
                    }
                });
        String keyPrefix = "ref_" + args[3] + "_";
        boolean isCompressed = "1".equals(args[4]);
        Thread thread = new Thread(new WriteLogThread(queue));
        thread.start();
        try (Connection connection = ConnectionFactory.createConnection(configuration);
             BufferedMutator bm = connection.getBufferedMutator(params)) {
            for (int i = 0; i < readThreadNum; i++) {
                int startId = i * 180000 / readThreadNum;
                int endId = startId + 180000 / readThreadNum - 1;
                pool.submit(new PutsThread(bm, js, queue, latch, count, keyPrefix, startId, endId, isCompressed));
            }

            latch.await();
        }
        queue.offer("数据量：" + count.get());
        queue.offer("总用时：" + (new Date().getTime() - startTime));
//        System.out.println("获取keys用时：" + keyCost);
        pool.shutdown();
        isDone = true;
        thread.join();
    }

    public static TreeSet<String> keys(String pattern, BinaryJedisCluster jedisCluster) {
        TreeSet<String> keys = new TreeSet<>();
        Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();
        for (String k : clusterNodes.keySet()) {
            JedisPool jp = clusterNodes.get(k);
            Jedis connection = jp.getResource();
            try {
                keys.addAll(connection.keys(pattern));
            } catch (Exception e) {
            } finally {
                connection.close();//用完一定要close这个链接！！！
            }
        }
        return keys;
    }

    static class WriteLogThread implements Runnable {
        private Queue<String> messages;

        public WriteLogThread(ConcurrentLinkedQueue<String> messages) {
            this.messages = messages;
        }

        @Override
        public void run() {
            try (FileOutputStream fis = new FileOutputStream("/home/wamdm/redisToHbase/log.txt")) {
                FileChannel fc = fis.getChannel();
                CharBuffer charBuffer = CharBuffer.allocate(2048);
                while (!RedisConsumer.isDone || !this.messages.isEmpty()) {
                    charBuffer.clear();
                    String message = this.messages.poll();
                    if (StringUtils.isEmpty(message))
                        continue;
                    message += "\n";
                    charBuffer.put(message.toCharArray());
                    charBuffer.flip();
                    ByteBuffer byteBuffer = Charset.defaultCharset().encode(charBuffer);
                    fc.write(byteBuffer);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
