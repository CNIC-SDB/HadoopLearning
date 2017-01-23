import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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

    public PutsThread(BufferedMutator bm, Set<String> ids, JedisCluster jedisCluster, String id, CountDownLatch latch, AtomicInteger count, String name) {
        this.bm = bm;
        this.ids = ids;
        this.jedisCluster = jedisCluster;
        this.id = id;
        this.latch = latch;
        this.count = count;
        this.name = name;
    }

    @Override
    public void run() {
        try {
            System.out.println("开始处理id：" + name);
            DecimalFormat df = new DecimalFormat("000000");
            DecimalFormat df1 = new DecimalFormat("000");
            for (String id : ids) {
                String[] splits = id.split("_");
                String splitNum = df.format(Integer.valueOf(splits[2])).substring(0, 2);
                String rowkeyPrefix = df1.format((Integer.valueOf(splits[1]) - 1) * 18 + Integer.valueOf(splitNum));
//                System.out.print(rowkeyPrefix+" ");
                List<String> values = this.jedisCluster.lrange(id, 0, this.jedisCluster.llen(id) - 1);
                List<Put> puts = new ArrayList<>(values.size());
                for (String value : values) {
                    String[] cols = value.split(" ");
                    Put put = new Put((rowkeyPrefix + id).getBytes());
                    put.setDurability(Durability.SKIP_WAL);
                    put.addColumn("cf".getBytes(), StarModel.CCD_NUM.getBytes(), Bytes.toBytes(cols[2]));
                    put.addColumn("cf".getBytes(), StarModel.IMAGEID.getBytes(), Bytes.toBytes(cols[3]));
                    put.addColumn("cf".getBytes(), StarModel.ZONE.getBytes(), Bytes.toBytes(cols[4]));
                    put.addColumn("cf".getBytes(), StarModel.RA.getBytes(), Bytes.toBytes(cols[5]));
                    put.addColumn("cf".getBytes(), StarModel.DEC.getBytes(), Bytes.toBytes(cols[6]));
                    put.addColumn("cf".getBytes(), StarModel.MAG.getBytes(), Bytes.toBytes(cols[7]));
                    put.addColumn("cf".getBytes(), StarModel.X_PIX.getBytes(), Bytes.toBytes(cols[8]));
                    put.addColumn("cf".getBytes(), StarModel.Y_PIX.getBytes(), Bytes.toBytes(cols[9]));
                    put.addColumn("cf".getBytes(), StarModel.RA_ERR.getBytes(), Bytes.toBytes(cols[10]));
                    put.addColumn("cf".getBytes(), StarModel.DEC_ERR.getBytes(), Bytes.toBytes(cols[11]));
                    put.addColumn("cf".getBytes(), StarModel.X.getBytes(), Bytes.toBytes(cols[12]));
                    put.addColumn("cf".getBytes(), StarModel.Y.getBytes(), Bytes.toBytes(cols[13]));
                    put.addColumn("cf".getBytes(), StarModel.Z.getBytes(), Bytes.toBytes(cols[14]));
                    put.addColumn("cf".getBytes(), StarModel.FLUX.getBytes(), Bytes.toBytes(cols[15]));
                    put.addColumn("cf".getBytes(), StarModel.FLUX_ERR.getBytes(), Bytes.toBytes(cols[16]));
                    put.addColumn("cf".getBytes(), StarModel.NORMMAG.getBytes(), Bytes.toBytes(cols[17]));
                    put.addColumn("cf".getBytes(), StarModel.FLAG.getBytes(), Bytes.toBytes(cols[18]));
                    put.addColumn("cf".getBytes(), StarModel.BACKGROUND.getBytes(), Bytes.toBytes(cols[19]));
                    put.addColumn("cf".getBytes(), StarModel.THRESHOLD.getBytes(), Bytes.toBytes(cols[20]));
                    put.addColumn("cf".getBytes(), StarModel.MAG_ERR.getBytes(), Bytes.toBytes(cols[21]));
                    put.addColumn("cf".getBytes(), StarModel.ELLIPTICITY.getBytes(), Bytes.toBytes(cols[22]));
                    put.addColumn("cf".getBytes(), StarModel.CLASS_STAR.getBytes(), Bytes.toBytes(cols[23]));
                    put.addColumn("cf".getBytes(), StarModel.ORIG_CATID.getBytes(), Bytes.toBytes(cols[24]));
                    puts.add(put);
                    this.count.incrementAndGet();
                }
                bm.mutate(puts);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            this.latch.countDown();
            System.out.println("id为：" + name + "完成");
        }

    }
}
