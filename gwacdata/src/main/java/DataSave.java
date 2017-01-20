import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * Created by Administrator on 2017/1/11 0011.
 */
public class DataSave {

    private Configuration conf = null;


    public void init() {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "WAMDM81,WAMDM82,WAMDM83,WAMDM84,WAMDM85");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.set("hbase.htable.threads.max", "8");
//        conf.set("hbase.master", "hdfs://10.0.83.149:60000");
//        conf.set("hbase.root.dir", "hdfs://10.0.83.149:9000/hbase");
    }


    public void saveData1(DataProduct dataProduct, int times) throws Exception {
        Connection connection = ConnectionFactory.createConnection(conf);
//        Table gwacData = null;
        BufferedMutator mutator = null;
        long startTime = System.currentTimeMillis();
//        int costTime = 0;
        long allCount = 0;
        try {
//            gwacData = connection.getTable(TableName.valueOf("g1"));
            BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf("g1"));
            params.writeBufferSize(20 * 1024 * 1024);
//            params.pool(Executors.newFixedThreadPool(16 * 2));
            mutator = connection.getBufferedMutator(params);
            System.out.println("客户端缓存：" + mutator.getWriteBufferSize());
            System.out.println("key-value max：" + params.getMaxKeyValueSize());

            while (times-- > 0) {
                List<StarModel> starModels = dataProduct.getStarModels();
                long start = System.currentTimeMillis();
                ArrayList<Put> puts = Lists.newArrayListWithCapacity(starModels.size());
                for (int i = 0; starModels.size() > i; i++) {
                    StarModel starModel = starModels.get(i);
                    int keyId = new Random().nextInt(170000);
                    String key = keyId + "." + new Random().nextInt(20) + "." + System.currentTimeMillis() / 1000;
                    Put put = new Put(Bytes.toBytes(key));
                    put.setDurability(Durability.SKIP_WAL);
                    put.addColumn("f".getBytes(), StarModel.CCD_NUM.getBytes(), Bytes.toBytes(starModel.getCcd_num()));
                    put.addColumn("f".getBytes(), StarModel.IMAGEID.getBytes(), Bytes.toBytes(starModel.getImageid()));
                    put.addColumn("f".getBytes(), StarModel.ZONE.getBytes(), Bytes.toBytes(starModel.getZone()));
                    put.addColumn("f".getBytes(), StarModel.RA.getBytes(), Bytes.toBytes(starModel.getRa()));
                    put.addColumn("f".getBytes(), StarModel.DEC.getBytes(), Bytes.toBytes(starModel.getDec()));
                    put.addColumn("f".getBytes(), StarModel.MAG.getBytes(), Bytes.toBytes(starModel.getMag()));
                    put.addColumn("f".getBytes(), StarModel.X_PIX.getBytes(), Bytes.toBytes(starModel.getX_pix()));
                    put.addColumn("f".getBytes(), StarModel.Y_PIX.getBytes(), Bytes.toBytes(starModel.getY_pix()));
                    put.addColumn("f".getBytes(), StarModel.RA_ERR.getBytes(), Bytes.toBytes(starModel.getRa_err()));
                    put.addColumn("f".getBytes(), StarModel.DEC_ERR.getBytes(), Bytes.toBytes(starModel.getDec_err()));
                    put.addColumn("f".getBytes(), StarModel.X.getBytes(), Bytes.toBytes(starModel.getX()));
                    put.addColumn("f".getBytes(), StarModel.Y.getBytes(), Bytes.toBytes(starModel.getY()));
                    put.addColumn("f".getBytes(), StarModel.Z.getBytes(), Bytes.toBytes(starModel.getZ()));
                    put.addColumn("f".getBytes(), StarModel.FLUX.getBytes(), Bytes.toBytes(starModel.getFlux()));
                    put.addColumn("f".getBytes(), StarModel.FLUX_ERR.getBytes(), Bytes.toBytes(starModel.getFlux_err()));
                    put.addColumn("f".getBytes(), StarModel.NORMMAG.getBytes(), Bytes.toBytes(starModel.getNormmag()));
                    put.addColumn("f".getBytes(), StarModel.FLAG.getBytes(), Bytes.toBytes(starModel.getFlag()));
                    put.addColumn("f".getBytes(), StarModel.BACKGROUND.getBytes(), Bytes.toBytes(starModel.getBackground()));
                    put.addColumn("f".getBytes(), StarModel.THRESHOLD.getBytes(), Bytes.toBytes(starModel.getThreshold()));
                    put.addColumn("f".getBytes(), StarModel.MAG_ERR.getBytes(), Bytes.toBytes(starModel.getMag_err()));
                    put.addColumn("f".getBytes(), StarModel.ELLIPTICITY.getBytes(), Bytes.toBytes(starModel.getEllipticity()));
                    put.addColumn("f".getBytes(), StarModel.CLASS_STAR.getBytes(), Bytes.toBytes(starModel.getClass_star()));
                    put.addColumn("f".getBytes(), StarModel.ORIG_CATID.getBytes(), Bytes.toBytes(starModel.getOrig_catid()));
                    puts.add(put);
                }
                mutator.mutate(puts);
//                gwacData.put(puts);
                long end = System.currentTimeMillis();
//                System.out.println("上传" + starModels.size() + "条数据，耗时：" + (end - start) + "毫秒");
//                costTime += end - start;
                allCount += starModels.size();
            }
//            System.out.println("上传" + allCount + "条数据，耗时：" + costTime + "毫秒");
        } finally {
//            gwacData.close();
            mutator.close();
            connection.close();
        }
        System.out.println("上传" + allCount + "条数据，耗时：" + (System.currentTimeMillis() - startTime) + "毫秒");
    }


    private static void batchInsert(String args[]) throws Exception {
        int cacheLength = 5;
        int cellSize = 5000;
        int times = 50;
        if (args.length == 3) {
            cacheLength = Integer.valueOf(args[0]);
            cellSize = Integer.valueOf(args[1]);
            times = Integer.valueOf(args[2]);
        }
        DataSave dataSave = new DataSave();
        DataProduct dataProduct = new DataProduct(cacheLength, cellSize);
        dataSave.init();
        dataSave.saveData1(dataProduct, times);


    }

    public static void main(String args[]) throws Exception {
        batchInsert(args);
    }
}
