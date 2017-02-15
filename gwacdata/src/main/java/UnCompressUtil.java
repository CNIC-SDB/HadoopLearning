import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by liuang on 2017/2/13.
 */
public class UnCompressUtil {

    /**
     * 解压redis中的数据
     *
     * @param compressedData 压缩的完整数据行，包含key
     * @param templateData   第一行完整数据，数组第一个是key
     * @return
     */
    public static String[] UnCompress(byte[] compressedData, String[] templateData) {
        List<String> originData = new ArrayList<>();
        String key = templateData[0];
        int compressStartIndex = (key + " ").getBytes().length;
        int compressDataStartIndex = compressStartIndex + 4;
        originData.add(key);
        try {
            byte[] headers = Arrays.copyOfRange(compressedData, compressStartIndex, compressDataStartIndex);
            for (int i = 0; i < 4; i++) {
                byte header = headers[i];
                byte[] bitsInHeader = new byte[8];
                if (header == 13) {
                    continue;
                }
                bitsInHeader = getBooleanArray(header);
                for (int j = 0; j < 8 && i * 8 + j < 24; j++) {
                    int columnNum = i * 8 + j;
                    if (bitsInHeader[j] == 0)
                        originData.add(templateData[columnNum + 1]);
                    else {
                        StringBuilder sb = new StringBuilder();
                        while (compressedData[compressDataStartIndex] != 14) {
                            int firstValue, secondValue;
                            if (compressedData[compressDataStartIndex] == 13) {
                                firstValue = secondValue = 0;
                            } else {
                                firstValue = (compressedData[compressDataStartIndex] >>> 4) & 15;
                                secondValue = compressedData[compressDataStartIndex] & 15;
                            }
                            switch (firstValue) {
                                case 10:
                                    sb.append(".");
                                    break;
                                case 11:
                                    sb.append("-");
                                    break;
                                case 12:
                                    break;
                                default:
                                    sb.append(firstValue);
                                    break;
                            }
                            switch (secondValue) {
                                case 10:
                                    sb.append(".");
                                    break;
                                case 11:
                                    sb.append("-");
                                    break;
                                case 12:
                                    break;
                                default:
                                    sb.append(secondValue);
                                    break;
                            }
                            compressDataStartIndex++;
                        }
                        compressDataStartIndex++;
                        float diffValue = Float.valueOf(sb.toString());
                        originData.add(String.valueOf(Float.valueOf(templateData[columnNum + 1]) + diffValue));
                    }
                }
            }
            if (originData.size() != templateData.length) {
                System.out.println("解压后数据列数不一致");
                return null;
            }
            String[] originDataArray = new String[originData.size()];
            return originData.toArray(originDataArray);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * 将byte转换为一个长度为8的byte数组，数组每个值代表一个bit
     */
    public static byte[] getBooleanArray(byte b) {
        byte[] array = new byte[8];
        for (int i = 7; i >= 0; i--) {
            array[i] = (byte) (b & 1);
            b = (byte) (b >> 1);
        }
        return array;
    }
}
