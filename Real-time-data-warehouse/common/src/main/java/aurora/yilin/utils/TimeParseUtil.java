package aurora.yilin.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/7/1
 */
public class TimeParseUtil {

    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * yyyy-MM-dd HH:mm:ss to 12312312312
     * @param dateStr
     * @return
     * @throws ParseException
     */
    public static long parseTimeFromYearToSeconds(String dateStr) throws ParseException {
        return simpleDateFormat.parse(dateStr).getTime();
    }

    /**
     * 1231231231 to yyyy-MM-dd HH:mm:ss
     * @param dateStr
     * @return
     */
    public static String FormatTimeFromYearToSeconds(String dateStr){
        return simpleDateFormat.format(dateStr);
    }

    /**
     * 根据需求设置simpleDateFormat对象
     * @param simpleDateFormat
     */
    public static void setDataFormat(SimpleDateFormat simpleDateFormat) {
        TimeParseUtil.simpleDateFormat = simpleDateFormat;
    }
}
