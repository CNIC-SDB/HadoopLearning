import data.gwac.v2.RedisConsumer;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import redis.clients.jedis.BinaryJedisCluster;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.*;

/**
 * Created by Administrator on 2017/1/12 0012.
 */
public class ByteTest {

    /**
     * 将byte转换为一个长度为8的byte数组，数组每个值代表bit
     */
    public static byte[] getBooleanArray(byte b) {
        byte[] array = new byte[8];
        for (int i = 7; i >= 0; i--) {
            array[i] = (byte) (b & 1);
            b = (byte) (b >> 1);
        }
        return array;
    }


    public void testBytes() {
        System.out.println("String 3456 length:" + Bytes.toBytes("3456").length);
        System.out.println("int 3456 length:" + Bytes.toBytes(3456).length);
        System.out.println("String 0.3976079116225744 length:" + Bytes.toBytes("0.3976079116225744").length);
        System.out.println("float " + 170000.1484216790791d + " length:" + Bytes.toBytes(0.3976079116225744d).length);
        System.out.println("float 123.1232546 length:" + Bytes.toBytes(123.1232546).length);
        System.out.println(RandomUtils.nextFloat(new Random(123)));
        System.out.println(System.currentTimeMillis());
        System.out.println(new Random().nextInt(170000) + "." + System.currentTimeMillis() / 100);
    }

    @Test
    public void generateSplitFiles() throws IOException {
        Set<HostAndPort> jedisClusterNodes = new HashSet<>();
        for (int start = 68, port = 7001; start < 88; start++) {
            for (int i = 0; i < 4; i++, port++) {
                jedisClusterNodes.add(new HostAndPort("10.0.83." + start, port));
            }
        }
        JedisCluster js = new JedisCluster(jedisClusterNodes);
        if (!js.exists("ref_1_136917"))
            System.out.println("not exist");
        Set<String> keys = RedisConsumer.keys("ref_1_136917", js);
        System.out.println(keys.size());
        for (String key : keys) {
            if (js.llen(key.getBytes()) > 1) {
                System.out.println(js.llen(key.getBytes()));
                List<byte[]> values = js.lrange(key.getBytes(), 0, 0);
                List<byte[]> valuesInBytes = js.lrange(key.getBytes(), 1, js.llen(key.getBytes()) - 1);
                System.out.println(new String(values.get(0)));
                byte[] bytes = valuesInBytes.get(48);
//                String s=new String(bytes, StandardCharsets.ISO_8859_1);
//                String test = StringEscapeUtils.escapeEcmaScript(s);
//                System.out.println(test);
//                int i = 1;
//                for (byte b : bytes) {
//                    System.out.print(byteToBit(b) + " ");
//                    if (i % 4 == 0)
//                        System.out.println();
//                    i++;
//                }
//                for(int i=0;i<valuesInBytes.size();i++){
//                    try{
//                        String[] originalValues= data.gwac.v2.UnCompressUtil.UnCompress(valuesInBytes.get(i),new String(values.get(0)).split(" "));
//                        System.out.print(i+" ");
//                        for(String string:originalValues)
//                            System.out.print(string+" ");
//                        System.out.println();
//                    }catch (Exception e){
//                        System.out.println(key+" "+i+"************");
//                    }
//
//                }

            }
        }

    }


    public void rowKewTest() {
        Set<HostAndPort> jedisClusterNodes = new HashSet<>();
        for (int start = 68, port = 7001; start < 88; start++) {
            for (int i = 0; i < 4; i++, port++) {
                jedisClusterNodes.add(new HostAndPort("10.0.83." + start, port));
            }
        }
        BinaryJedisCluster js = new BinaryJedisCluster(jedisClusterNodes);
        Set<String> keys = RedisConsumer.keys("ref_2_*", js);
        System.out.println(keys.size());
        Map<String, Integer> map = new HashMap<>(256);
        DecimalFormat df = new DecimalFormat("000000");
        DecimalFormat df1 = new DecimalFormat("0000");
        for (String key : keys) {
            String[] splits = key.split("_");
            String splitNum = df.format(Integer.valueOf(splits[2])).substring(0, 3);
            String rowkeyPrefix = df1.format((Integer.valueOf(splits[1]) - 1) * 180 + Integer.valueOf(splitNum));
            if (map.containsKey(rowkeyPrefix))
                map.put(rowkeyPrefix, map.get(rowkeyPrefix) + 1);
            else {
                map.put(rowkeyPrefix, 1);
            }
        }
        System.out.println(map.size());
        int i = 1;
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            System.out.print(entry.getKey() + ":" + entry.getValue() + " ");

            if (i % 5 == 0)
                System.out.println();
            i++;
        }
    }
    public String byteToBit(byte b) {
        return ""
                + (byte) ((b >> 7) & 0x1) + (byte) ((b >> 6) & 0x1)
                + (byte) ((b >> 5) & 0x1) + (byte) ((b >> 4) & 0x1)
                + (byte) ((b >> 3) & 0x1) + (byte) ((b >> 2) & 0x1)
                + (byte) ((b >> 1) & 0x1) + (byte) ((b >> 0) & 0x1);
    }


    public void readFile() {
        String filePath = "C:\\Users\\liuang\\Desktop\\RA240_DEC10_sqd225-ccd2-301920.cat";
        try {
            List<String> content = Files.readAllLines(Paths.get(filePath), Charset.defaultCharset());
            System.out.println(content.size());
            String[] datas = content.get(0).split(" ");
            for (String data : datas) {
                System.out.println(data);
            }
            System.out.println(content.get(0).split(" ").length);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
