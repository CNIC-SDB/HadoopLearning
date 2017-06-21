import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by liuang on 2017/5/15.
 */
public class FileConsumer {
    static volatile Boolean isDone = false;

    public static void main(String[] args) throws IOException, InterruptedException {
        long startTime = new Date().getTime();
        final ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();
        queue.offer("开始：" + new Date().toString());
        AtomicInteger count = new AtomicInteger(0);
        int readThreadNum = 30;
        CountDownLatch latch = new CountDownLatch(readThreadNum);
        ExecutorService pool = Executors.newFixedThreadPool(readThreadNum);
        CompletionService completionService = new ExecutorCompletionService(pool);
        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource("/home/wamdm/hbase-1.2.4/conf/hbase-site.xml");
        configuration.addResource("/home/wamdm/hadoop-2.7.3/etc/hadoop/core-site.xml");
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf("gwacdata3600"));
        params.writeBufferSize(12000)
                .pool(Executors.newFixedThreadPool(60))
                .listener(new BufferedMutator.ExceptionListener() {
                    @Override
                    public void onException(RetriesExhaustedWithDetailsException exception, BufferedMutator mutator) throws RetriesExhaustedWithDetailsException {
                        String ex = exception.getLocalizedMessage();
                        queue.offer(ex);
                        for (StackTraceElement stackTraceElement : exception.getStackTrace()) {
                            queue.offer("at " + stackTraceElement);
                        }
                    }
                });
        Thread thread = new Thread(new RedisConsumer.WriteLogThread(queue));
        thread.start();
        try (Connection connection = ConnectionFactory.createConnection(configuration);
             BufferedMutator bm = connection.getBufferedMutator(params)) {
            for (int i = 0; i < 30; i++) {
                int startId = i * 162 + 3160;
                int endId = startId + 161;
                if (i == 29)
                    endId += 20;
                completionService.submit(new PutThread(bm, count, "ref_1_", startId, endId, queue), true);
            }
            for (int i = 0; i < 30; i++) {
                completionService.take();
            }
        }
        queue.offer("数据量：" + count.get());
        queue.offer("总用时：" + (new Date().getTime() - startTime));
        pool.shutdown();
        isDone = true;
        thread.join();
    }

    static class PutThread implements Runnable {
        private BufferedMutator bm;
        private AtomicInteger count;
        private String name;
        private int startId;
        private int endId;
        private ConcurrentLinkedQueue<String> messages;

        public PutThread(BufferedMutator bm, AtomicInteger count, String name, int startId, int endId, ConcurrentLinkedQueue<String> messages) {
            this.bm = bm;
            this.count = count;
            this.name = name;
            this.startId = startId;
            this.endId = endId;
            this.messages = messages;
        }

        @Override
        public void run() {
            this.messages.offer("开始处理id：" + name + "(" + startId + "-" + endId + ")");
            String dataDirectoryPath = "/home/wamdm/gwac/data_backup";
            DecimalFormat df = new DecimalFormat("000000");
            DecimalFormat df1 = new DecimalFormat("0000");
            List<String> datas = null;
            for (int i = startId; i <= endId; i++) {
                String dataFilePath = dataDirectoryPath + "/" + "RA240_DEC10_sqd225-ccd1-" + i + ".cat";
                File dataFile = new File(dataFilePath);
                if (!dataFile.exists())
                    continue;
                String id = this.name + i;
                System.out.println(dataFilePath);
                String[] splits = id.split("_");
                String splitNum = df.format(Integer.valueOf(splits[2])).substring(0, 3);
                String rowkeyPrefix = df1.format((Integer.valueOf(splits[1]) - 1) * 180 + Integer.valueOf(splitNum));
                try {
                    datas = readFileContent(dataFile);
                    long size = datas.size();
                    System.out.println(size + " " + rowkeyPrefix);
                    List<Put> puts = new ArrayList<>((int) size);
                    for (int j = 0; j < size; j++) {
                        String[] templateData = datas.get(j).split(" ");
                        Put put = new Put((rowkeyPrefix + id + "_" + templateData[23] + j).getBytes());
                        put.setDurability(Durability.SKIP_WAL);
                        generatePut(templateData, put);
                        puts.add(put);
                        this.count.incrementAndGet();
                    }
                    bm.mutate(puts);
                    bm.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                    continue;
                }

            }
            System.out.println("id为：" + name + "(" + startId + "-" + endId + ")完成");
            this.messages.offer("id为：" + name + "(" + startId + "-" + endId + ")完成");
        }

        public List<String> readFileContent(File file) throws IOException {

            return Files.readAllLines(Paths.get(file.getPath()), Charset.defaultCharset());
        }

        public void generatePut(String[] cols, Put put) {
            put.addColumn("cf".getBytes(), StarModel.CCD_NUM.getBytes(), Bytes.toBytes(cols[0]));
            put.addColumn("cf".getBytes(), StarModel.IMAGEID.getBytes(), Bytes.toBytes(cols[1]));
            put.addColumn("cf".getBytes(), StarModel.ZONE.getBytes(), Bytes.toBytes(cols[2]));
            put.addColumn("cf".getBytes(), StarModel.RA.getBytes(), Bytes.toBytes(cols[3]));
            put.addColumn("cf".getBytes(), StarModel.DEC.getBytes(), Bytes.toBytes(cols[4]));
            put.addColumn("cf".getBytes(), StarModel.MAG.getBytes(), Bytes.toBytes(cols[5]));
            put.addColumn("cf".getBytes(), StarModel.X_PIX.getBytes(), Bytes.toBytes(cols[6]));
            put.addColumn("cf".getBytes(), StarModel.Y_PIX.getBytes(), Bytes.toBytes(cols[7]));
            put.addColumn("cf".getBytes(), StarModel.RA_ERR.getBytes(), Bytes.toBytes(cols[8]));
            put.addColumn("cf".getBytes(), StarModel.DEC_ERR.getBytes(), Bytes.toBytes(cols[9]));
            put.addColumn("cf".getBytes(), StarModel.X.getBytes(), Bytes.toBytes(cols[10]));
            put.addColumn("cf".getBytes(), StarModel.Y.getBytes(), Bytes.toBytes(cols[11]));
            put.addColumn("cf".getBytes(), StarModel.Z.getBytes(), Bytes.toBytes(cols[12]));
            put.addColumn("cf".getBytes(), StarModel.FLUX.getBytes(), Bytes.toBytes(cols[13]));
            put.addColumn("cf".getBytes(), StarModel.FLUX_ERR.getBytes(), Bytes.toBytes(cols[14]));
            put.addColumn("cf".getBytes(), StarModel.NORMMAG.getBytes(), Bytes.toBytes(cols[15]));
            put.addColumn("cf".getBytes(), StarModel.FLAG.getBytes(), Bytes.toBytes(cols[16]));
            put.addColumn("cf".getBytes(), StarModel.BACKGROUND.getBytes(), Bytes.toBytes(cols[17]));
            put.addColumn("cf".getBytes(), StarModel.THRESHOLD.getBytes(), Bytes.toBytes(cols[18]));
            put.addColumn("cf".getBytes(), StarModel.MAG_ERR.getBytes(), Bytes.toBytes(cols[19]));
            put.addColumn("cf".getBytes(), StarModel.ELLIPTICITY.getBytes(), Bytes.toBytes(cols[20]));
            put.addColumn("cf".getBytes(), StarModel.CLASS_STAR.getBytes(), Bytes.toBytes(cols[21]));
            put.addColumn("cf".getBytes(), StarModel.ORIG_CATID.getBytes(), Bytes.toBytes(cols[22]));
        }
    }
}
