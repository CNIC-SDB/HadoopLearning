import org.apache.commons.lang.SystemUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * Created by Administrator on 2017/1/12 0012.
 */
public class ByteTest {

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
        Set<String> keys = RedisConsumer.keys("ref_1_???", js);
        System.out.println(keys.size());
    }
}
