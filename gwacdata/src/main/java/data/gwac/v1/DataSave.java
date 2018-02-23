package data.gwac.v1;

import com.google.common.collect.Lists;
import data.gwac.StarModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by Administrator on 2017/1/11 0011.
 */
public class DataSave {

    private Configuration conf = null;

    private static void batchInsert(String args[]) throws Exception {
        int cacheLength = 5;
        int cellSize = 100;
        int times = 1;
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

    public void init() {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "WAMDM81,WAMDM82,WAMDM83,WAMDM84,WAMDM85");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.set("hbase.master", "hdfs://10.0.83.149:60000");
//        conf.set("hbase.root.dir", "hdfs://10.0.83.149:9000/hbase");
    }

    public void saveData1(DataProduct dataProduct, int times) throws Exception {
        Connection connection = ConnectionFactory.createConnection(conf);
        Table gwacData = null;
        try {
            gwacData = connection.getTable(TableName.valueOf("gwacsplit"));
            int costTime = 0;
            long allCount = 0;
            while (times-- > 0) {
                List<StarModel> starModels = dataProduct.getStarModels();
                long start = System.currentTimeMillis();
                ArrayList<Put> puts = Lists.newArrayListWithCapacity(starModels.size());
                for (int i = 0; starModels.size() > i; i++) {

                    StarModel starModel = starModels.get(i);
                    Put put = new Put(Bytes.toBytes(starModel.getRowKeyId()));
                    put.setWriteToWAL(false);
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
                gwacData.put(puts);
                long end = System.currentTimeMillis();
                System.out.println("上传" + starModels.size() + "条数据，耗时：" + (end - start) + "毫秒");
                costTime += end - start;
                allCount += starModels.size();
            }
            System.out.println("上传" + allCount + "条数据，耗时：" + costTime + "毫秒");
        } finally {
            gwacData.close();
            connection.close();
        }
    }
}
