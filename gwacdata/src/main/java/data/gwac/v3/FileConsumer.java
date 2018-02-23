package data.gwac.v3;

import data.gwac.WriteLogThread;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


public class FileConsumer {
    public static volatile Boolean isDone = true;

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
        isDone = false;
        Thread thread = new Thread(new WriteLogThread(queue));
        thread.start();
        try (Connection connection = ConnectionFactory.createConnection(configuration);
             BufferedMutator bm = connection.getBufferedMutator(params)) {
            for (int i = 0; i < 30; i++) {
                int startId = i * 162 + 3160;
                int endId = startId + 161;
                if (i == 29)
                    endId += 20;
                completionService.submit(new PutThread(bm, count, "ref_1_", startId, endId, queue, latch), true);
            }
            for (int i = 0; i < 30; i++) {
                completionService.take();
            }
        }
        latch.await();
        queue.offer("数据量：" + count.get());
        queue.offer("总用时：" + (new Date().getTime() - startTime));
        pool.shutdown();
        isDone = true;
        thread.join();
    }
}
