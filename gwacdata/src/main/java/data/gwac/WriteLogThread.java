package data.gwac;

import data.gwac.v2.RedisConsumer;
import data.gwac.v3.FileConsumer;
import org.apache.commons.lang3.StringUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class WriteLogThread implements Runnable {
    private Queue<String> messages;

    public WriteLogThread(ConcurrentLinkedQueue<String> messages) {
        this.messages = messages;
    }

    @Override
    public void run() {
        try (FileOutputStream fis = new FileOutputStream("/home/wamdm/redisToHbase/log.txt")) {
            FileChannel fc = fis.getChannel();
//            CharBuffer charBuffer = CharBuffer.allocate(2048);
            ByteBuffer byteBuffer = ByteBuffer.allocate(2048);
            while (!RedisConsumer.isDone || !FileConsumer.isDone || !this.messages.isEmpty()) {
                String message = this.messages.poll();
                if (StringUtils.isEmpty(message)) {
                    continue;
                }
                message += "\n";
//                charBuffer.clear();
//                charBuffer.put(message.toCharArray());
//                charBuffer.flip();
//                ByteBuffer byteBuffer = Charset.defaultCharset().encode(charBuffer);
                byteBuffer.clear();
                byteBuffer.put(message.getBytes());
                byteBuffer.flip();
                fc.write(byteBuffer);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
