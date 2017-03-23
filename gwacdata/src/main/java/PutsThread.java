import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by liuang on 2017/1/22.
 */
public class PutsThread implements Runnable {
    private BufferedMutator bm;
    private Set<String> ids;
    private JedisCluster jedisCluster;
    private String id;
    private CountDownLatch latch;
    private AtomicInteger count;
    private String name;
    private int startId;
    private int endId;
    private ConcurrentLinkedQueue<String> messages;

    public PutsThread(BufferedMutator bm, JedisCluster jedisCluster, ConcurrentLinkedQueue<String> messages,
                      CountDownLatch latch, AtomicInteger count, String name, int startId, int endId) {
        this.bm = bm;
        this.jedisCluster = jedisCluster;
        this.latch = latch;
        this.count = count;
        this.name = name;
        this.startId = startId;
        this.endId = endId;
        this.messages = messages;
    }

    @Override
    public void run() {
        try {
            this.messages.offer("开始处理id：" + name + "(" + startId + "-" + endId + ")");
            System.out.println("开始处理id：" + name + "(" + startId + "-" + endId + ")");
            DecimalFormat df = new DecimalFormat("000000");
            DecimalFormat df1 = new DecimalFormat("0000");
            for (int i = startId; i <= endId; i++) {
                String id = this.name + i;
                if (!jedisCluster.exists(id))
                    continue;
                String[] splits = id.split("_");
                String splitNum = df.format(Integer.valueOf(splits[2])).substring(0, 3);
                String rowkeyPrefix = df1.format((Integer.valueOf(splits[1]) - 1) * 180 + Integer.valueOf(splitNum));
                List<String> values = this.jedisCluster.lrange(id, 0, 0);
                String[] templateData = values.get(0).split(" ");
                long size = this.jedisCluster.llen(id);
                List<Put> puts = new ArrayList<>((int) size);
                Put put = new Put((rowkeyPrefix + id + "_" + templateData[24]).getBytes());
                put.setDurability(Durability.SKIP_WAL);
                generatePut(templateData, put);
                puts.add(put);
                if (size > 1) {
                    List<byte[]> compressDataInBytes = this.jedisCluster.lrange(id.getBytes(), 1, size - 1);
                    for (byte[] value : compressDataInBytes) {
                        String[] cols = UnCompressUtil.UnCompress(value, templateData);
                        if (cols == null) {
                            this.messages.offer("解压失败：" + id);
                            continue;
                        }
                        put = new Put((rowkeyPrefix + id + "_" + cols[24]).getBytes());
                        put.setDurability(Durability.SKIP_WAL);
                        generatePut(cols, put);
                        puts.add(put);
                        this.count.incrementAndGet();
                    }
                }
                bm.mutate(puts);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            this.latch.countDown();
            this.messages.offer("id为：" + name + "(" + startId + "-" + endId + ")完成");
        }

    }

    public void generatePut(String[] cols, Put put) {
        put.addColumn("cf".getBytes(), StarModel.CCD_NUM.getBytes(), Bytes.toBytes(cols[1]));
        put.addColumn("cf".getBytes(), StarModel.IMAGEID.getBytes(), Bytes.toBytes(cols[2]));
        put.addColumn("cf".getBytes(), StarModel.ZONE.getBytes(), Bytes.toBytes(cols[3]));
        put.addColumn("cf".getBytes(), StarModel.RA.getBytes(), Bytes.toBytes(cols[4]));
        put.addColumn("cf".getBytes(), StarModel.DEC.getBytes(), Bytes.toBytes(cols[5]));
        put.addColumn("cf".getBytes(), StarModel.MAG.getBytes(), Bytes.toBytes(cols[6]));
        put.addColumn("cf".getBytes(), StarModel.X_PIX.getBytes(), Bytes.toBytes(cols[7]));
        put.addColumn("cf".getBytes(), StarModel.Y_PIX.getBytes(), Bytes.toBytes(cols[8]));
        put.addColumn("cf".getBytes(), StarModel.RA_ERR.getBytes(), Bytes.toBytes(cols[9]));
        put.addColumn("cf".getBytes(), StarModel.DEC_ERR.getBytes(), Bytes.toBytes(cols[10]));
        put.addColumn("cf".getBytes(), StarModel.X.getBytes(), Bytes.toBytes(cols[11]));
        put.addColumn("cf".getBytes(), StarModel.Y.getBytes(), Bytes.toBytes(cols[12]));
        put.addColumn("cf".getBytes(), StarModel.Z.getBytes(), Bytes.toBytes(cols[13]));
        put.addColumn("cf".getBytes(), StarModel.FLUX.getBytes(), Bytes.toBytes(cols[14]));
        put.addColumn("cf".getBytes(), StarModel.FLUX_ERR.getBytes(), Bytes.toBytes(cols[15]));
        put.addColumn("cf".getBytes(), StarModel.NORMMAG.getBytes(), Bytes.toBytes(cols[16]));
        put.addColumn("cf".getBytes(), StarModel.FLAG.getBytes(), Bytes.toBytes(cols[17]));
        put.addColumn("cf".getBytes(), StarModel.BACKGROUND.getBytes(), Bytes.toBytes(cols[18]));
        put.addColumn("cf".getBytes(), StarModel.THRESHOLD.getBytes(), Bytes.toBytes(cols[19]));
        put.addColumn("cf".getBytes(), StarModel.MAG_ERR.getBytes(), Bytes.toBytes(cols[20]));
        put.addColumn("cf".getBytes(), StarModel.ELLIPTICITY.getBytes(), Bytes.toBytes(cols[21]));
        put.addColumn("cf".getBytes(), StarModel.CLASS_STAR.getBytes(), Bytes.toBytes(cols[22]));
        put.addColumn("cf".getBytes(), StarModel.ORIG_CATID.getBytes(), Bytes.toBytes(cols[23]));
    }
}
