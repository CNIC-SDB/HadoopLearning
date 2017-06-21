/**
 * Created by Administrator on 2017/1/11 0011.
 */
public class StarModel {
    /**
     * starTable表
     * "star_id",StringType
     * "ccd_num",IntegerType
     * "imageid",IntegerType
     * "zone",IntegerType
     * "ra",DoubleType
     * "dec",DoubleType
     * "mag",DoubleType
     * "x_pix",DoubleType
     * "y_pix",DoubleType
     * "ra_err",DoubleType
     * "dec_err",DoubleType
     * "x",DoubleType
     * "y",DoubleType
     * "z",DoubleType
     * "flux",DoubleType
     * "flux_err",DoubleType
     * "normmag",DoubleType
     * "flag",DoubleType
     * "background",DoubleType
     * "threshold",DoubleType
     * "mag_err",DoubleType
     * "ellipticity",DoubleType
     * "class_star",DoubleType
     * "orig_catid",IntegerType
     * "class_star",IntegerType
     */

    public final static String CCD_NUM = "ccd_num";
    public final static String IMAGEID = "imageid";
    public final static String ZONE = "zone";
    public final static String RA = "ra";
    public final static String DEC = "dec";
    public final static String MAG = "mag";
    public final static String X_PIX = "x_pix";
    public final static String Y_PIX = "y_pix";
    public final static String RA_ERR = "ra_err";
    public final static String DEC_ERR = "dec_err";
    public final static String X = "x";
    public final static String Y = "y";
    public final static String Z = "z";
    public final static String FLUX = "flux";
    public final static String FLUX_ERR = "flux_err";
    public final static String NORMMAG = "normmag";
    public final static String FLAG = "flag";
    public final static String BACKGROUND = "background";
    public final static String THRESHOLD = "threshold";
    public final static String MAG_ERR = "mag_err";
    public final static String ELLIPTICITY = "ellipticity";
    public final static String CLASS_STAR = "class_star";
    public final static String ORIG_CATID = "orig_catid";


    private String rowKeyId; //行键ID
    private int ccd_num;
    private int imageid;
    private float zone;
    private float ra;
    private float dec;
    private float mag;
    private float x_pix;
    private float y_pix;
    private float ra_err;
    private float dec_err;
    private double x;
    private double y;
    private double z;
    private float flux;
    private float flux_err;
    private float normmag;
    private float flag;
    private float background;
    private float threshold;
    private float mag_err;
    private float ellipticity;
    private int class_star;
    private int orig_catid;


    public StarModel(String rowKeyId, int ccd_num, int imageid, float zone, float ra, float dec, float mag,
                     float x_pix, float y_pix, float ra_err, float dec_err, double x, double y, double z,
                     float flux, float flux_err, float normmag, float flag, float background, float threshold,
                     float mag_err, float ellipticity, int class_star, int orig_catid) {
        this.rowKeyId = rowKeyId;
        this.ccd_num = ccd_num;
        this.imageid = imageid;
        this.zone = zone;
        this.ra = ra;
        this.dec = dec;
        this.mag = mag;
        this.x_pix = x_pix;
        this.y_pix = y_pix;
        this.ra_err = ra_err;
        this.dec_err = dec_err;
        this.x = x;
        this.y = y;
        this.z = z;
        this.flux = flux;
        this.flux_err = flux_err;
        this.normmag = normmag;
        this.flag = flag;
        this.background = background;
        this.threshold = threshold;
        this.mag_err = mag_err;
        this.ellipticity = ellipticity;
        this.class_star = class_star;
        this.orig_catid = orig_catid;
    }

    public String getRowKeyId() {
        return rowKeyId;
    }

    public void setRowKeyId(String rowKeyId) {
        this.rowKeyId = rowKeyId;
    }

    public int getCcd_num() {
        return ccd_num;
    }

    public void setCcd_num(int ccd_num) {
        this.ccd_num = ccd_num;
    }

    public int getImageid() {
        return imageid;
    }

    public void setImageid(int imageid) {
        this.imageid = imageid;
    }

    public float getZone() {
        return zone;
    }

    public void setZone(float zone) {
        this.zone = zone;
    }

    public float getRa() {
        return ra;
    }

    public void setRa(float ra) {
        this.ra = ra;
    }

    public float getDec() {
        return dec;
    }

    public void setDec(float dec) {
        this.dec = dec;
    }

    public float getMag() {
        return mag;
    }

    public void setMag(float mag) {
        this.mag = mag;
    }

    public float getX_pix() {
        return x_pix;
    }

    public void setX_pix(float x_pix) {
        this.x_pix = x_pix;
    }

    public float getY_pix() {
        return y_pix;
    }

    public void setY_pix(float y_pix) {
        this.y_pix = y_pix;
    }

    public float getRa_err() {
        return ra_err;
    }

    public void setRa_err(float ra_err) {
        this.ra_err = ra_err;
    }

    public float getDec_err() {
        return dec_err;
    }

    public void setDec_err(float dec_err) {
        this.dec_err = dec_err;
    }

    public double getX() {
        return x;
    }

    public void setX(double x) {
        this.x = x;
    }

    public double getY() {
        return y;
    }

    public void setY(double y) {
        this.y = y;
    }

    public double getZ() {
        return z;
    }

    public void setZ(double z) {
        this.z = z;
    }

    public float getFlux() {
        return flux;
    }

    public void setFlux(float flux) {
        this.flux = flux;
    }

    public float getFlux_err() {
        return flux_err;
    }

    public void setFlux_err(float flux_err) {
        this.flux_err = flux_err;
    }

    public float getNormmag() {
        return normmag;
    }

    public void setNormmag(float normmag) {
        this.normmag = normmag;
    }

    public float getFlag() {
        return flag;
    }

    public void setFlag(float flag) {
        this.flag = flag;
    }

    public float getBackground() {
        return background;
    }

    public void setBackground(float background) {
        this.background = background;
    }

    public float getThreshold() {
        return threshold;
    }

    public void setThreshold(float threshold) {
        this.threshold = threshold;
    }

    public float getMag_err() {
        return mag_err;
    }

    public void setMag_err(float mag_err) {
        this.mag_err = mag_err;
    }

    public float getEllipticity() {
        return ellipticity;
    }

    public void setEllipticity(float ellipticity) {
        this.ellipticity = ellipticity;
    }

    public int getClass_star() {
        return class_star;
    }

    public void setClass_star(int class_star) {
        this.class_star = class_star;
    }

    public int getOrig_catid() {
        return orig_catid;
    }

    public void setOrig_catid(int orig_catid) {
        this.orig_catid = orig_catid;
    }
}
