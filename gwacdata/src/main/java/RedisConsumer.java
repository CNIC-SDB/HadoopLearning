import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by liuang on 2017/1/19.
 */
public class RedisConsumer {
    public static void main(String[] args) throws IOException, InterruptedException {
        long startTime = new Date().getTime();
        System.out.println("开始：" + startTime);
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
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf("gwacdata1"));
        params.writeBufferSize(Long.valueOf(args[1]))
                .pool(Executors.newFixedThreadPool(Integer.valueOf(args[2])))
                .listener(new BufferedMutator.ExceptionListener() {
                    @Override
                    public void onException(RetriesExhaustedWithDetailsException exception, BufferedMutator mutator) throws RetriesExhaustedWithDetailsException {
                        exception.printStackTrace();
                    }
                });
        long keyCost = 0;
        String keyPrefix = "ref_" + args[3] + "_";
        try (Connection connection = ConnectionFactory.createConnection(configuration);
             BufferedMutator bm = connection.getBufferedMutator(params)) {
//            Set<String> keys;
//            long getKeyStart = new Date().getTime();
//            keys = keys("ref_1_?", js);
//            keys.addAll(keys("ref_1_??", js));
//            keys.addAll(keys("ref_1_???", js));
//            keys.addAll(keys("ref_1_????", js));
//            long getKeyEnd = new Date().getTime();
//            keyCost += (getKeyEnd - getKeyStart);
//            pool.submit(new PutsThread(bm, keys, js, null, latch, count, keyPrefix,));
//            String pattern;
//            for (int i = 1; i < 18; i++) {
//                pattern = i + "????";
//                getKeyStart = new Date().getTime();
//                keys = keys("ref_1_" + pattern, js);
//                getKeyEnd = new Date().getTime();
//                keyCost += (getKeyEnd - getKeyStart);
//                pool.submit(new PutsThread(bm, keys, js, null, latch, count, "ref_1_" + pattern));
//            }
            for (int i = 0; i < readThreadNum; i++) {
                int startId = i * 180000 / readThreadNum;
                int endId = startId + 180000 / readThreadNum - 1;
                pool.submit(new PutsThread(bm, null, js, null, latch, count, keyPrefix, startId, endId));
            }
            latch.await();
        }
        System.out.println("数据量：" + count.get());
        System.out.println("总用时：" + (new Date().getTime() - startTime));
//        System.out.println("获取keys用时：" + keyCost);
        pool.shutdown();
    }

    public static TreeSet<String> keys(String pattern, JedisCluster jedisCluster) {
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
}
