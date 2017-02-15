import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
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

    @Test
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
        Set<String> keys = RedisConsumer.keys("ref_1_80037", js);
        System.out.println(keys.size());
        for (String key : keys) {
            if (js.llen(key) > 1) {
                List<String> values = js.lrange(key, 0, 0);
                List<byte[]> valuesInBytes = js.lrange(key.getBytes(), 1, js.llen(key) - 1);
                System.out.println(values.get(0));
                byte[] bytes = valuesInBytes.get(272);
                int i = 1;
                for (byte b : bytes) {
                    System.out.print(byteToBit(b) + " ");
                    if (i % 4 == 0)
                        System.out.println();
                    i++;
                }
//                for(int i=0;i<valuesInBytes.size();i++){
//                    try{
//                        String[] originalValues= UnCompressUtil.UnCompress(valuesInBytes.get(i),values.get(0).split(" "));
//                        System.out.print(i+" ");
//                        for(String string:originalValues)
//                            System.out.print(string+" ");
//                        System.out.println();
//                    }catch (Exception e){
//                        System.out.println(i+"************");
//                    }
//
//                }

                break;
            }
        }

    }

    public String byteToBit(byte b) {
        return ""
                + (byte) ((b >> 7) & 0x1) + (byte) ((b >> 6) & 0x1)
                + (byte) ((b >> 5) & 0x1) + (byte) ((b >> 4) & 0x1)
                + (byte) ((b >> 3) & 0x1) + (byte) ((b >> 2) & 0x1)
                + (byte) ((b >> 1) & 0x1) + (byte) ((b >> 0) & 0x1);
    }
}
