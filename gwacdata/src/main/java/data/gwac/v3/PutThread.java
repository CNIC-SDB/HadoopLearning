package data.gwac.v3;

import data.gwac.StarModel;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class PutThread implements Runnable {
    private BufferedMutator bm;
    private AtomicInteger count;
    private String name;
    private int startId;
    private int endId;
    private ConcurrentLinkedQueue<String> messages;
    private CountDownLatch latch;

    public PutThread(BufferedMutator bm, AtomicInteger count,
                     String name, int startId, int endId,
                     ConcurrentLinkedQueue<String> messages,
                     CountDownLatch latch) {
        this.bm = bm;
        this.count = count;
        this.name = name;
        this.startId = startId;
        this.endId = endId;
        this.messages = messages;
        this.latch = latch;
    }

    @Override
    public void run() {
        try {
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
                        String key = rowkeyPrefix + id + "_" + templateData[23] + j;
                        Put put = new Put(key.getBytes());
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
        } finally {
            latch.countDown();
        }
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
