import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by Administrator on 2017/1/11 0011.
 */
public class DataProduct {
    private int cacheLength = 1;
    private int cellSize = 100;
    private LinkedBlockingQueue<List<StarModel>> starListQueue = null;
    private ScheduledExecutorService scheduledExecutorService = null;

    public DataProduct(final int cacheLength, final int cellSize) {
        if (cacheLength > 1) {
            this.cacheLength = cacheLength;
        }
        if (cellSize > 100) {
            this.cellSize = cellSize;
        }
        starListQueue = Queues.newLinkedBlockingQueue(cacheLength);
        scheduledExecutorService = Executors.newScheduledThreadPool(1);
        initCache();
    }

    private void initCache() {
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            public void run() {
                int createCount = cacheLength - starListQueue.size();
                while (createCount-- > 0) {
                    try {
                        starListQueue.put(createStarList(cellSize));
                    } catch (InterruptedException e) {
                    }
                }
            }
        }, 100L, 100L, TimeUnit.MILLISECONDS);
    }

    public List<StarModel> getStarModels() {
        try {
            return starListQueue.take();
        } catch (InterruptedException e) {
        }
        return null;
    }

    public List<StarModel> createStarList(int count) {
        ArrayList<StarModel> starList = Lists.newArrayListWithCapacity(count);
        for (int i = 0; i < count; i++) {
            starList.add(createStar());
        }
        return starList;
    }

    public StarModel createStar() {
        String rowKeyId = null;
//        String rowKeyId = new Random().nextInt(170000) + "." + new Random().nextInt(20) + "." + System.currentTimeMillis() / 1000;
        int ccd_num = new Random().nextInt(10000);
        int imageid = new Random().nextInt(10000);
        float zone = new Random().nextFloat();
        float ra = new Random().nextFloat();
        float dec = new Random().nextFloat();
        float mag = new Random().nextFloat();
        float x_pix = new Random().nextFloat();
        float y_pix = new Random().nextFloat();
        float ra_err = new Random().nextFloat();
        float dec_err = new Random().nextFloat();
        double x = new Random().nextDouble();
        double y = new Random().nextDouble();
        double z = new Random().nextDouble();
        float flux = new Random().nextFloat();
        float flux_err = new Random().nextFloat();
        float normmag = new Random().nextFloat();
        float flag = new Random().nextFloat();
        float background = new Random().nextFloat();
        float threshold = new Random().nextFloat();
        float mag_err = new Random().nextFloat();
        float ellipticity = new Random().nextFloat();
        int class_star = new Random().nextInt(10000);
        int orig_catid = new Random().nextInt(10000);

        return new StarModel(rowKeyId, ccd_num, imageid, zone, ra, dec,
                mag, x_pix, y_pix, ra_err, dec_err, x, y, z,
                flux, flux_err, normmag, flag, background, threshold,
                mag_err, ellipticity, class_star, orig_catid);
    }


}
