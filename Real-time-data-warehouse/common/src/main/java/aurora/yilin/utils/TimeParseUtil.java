package aurora.yilin.utils;

import javafx.scene.input.DataFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/7/1
 */
public class TimeParseUtil {
    private static final Logger log = LoggerFactory.getLogger(TimeParseUtil.class);

    //非线程安全
    private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    //线程安全
    private static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * yyyy-MM-dd HH:mm:ss to 12312312312
     * @param dateStr
     * @return
     * @throws ParseException
     */
    public static long parseTimeFromYearToSeconds(String dateStr){
        try {
            return simpleDateFormat.parse(dateStr).getTime();
        }catch (ParseException pex){
            log.error(pex.getMessage());
        }
        return System.currentTimeMillis();

    }

    public static long parseTimeThreadSafety(String dateStr){
        LocalDateTime localDateTime = LocalDateTime.parse(dateStr, dateTimeFormatter);
        return localDateTime.toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
    }

    /**
     * 1231231231 to yyyy-MM-dd HH:mm:ss
     * @param dateStr
     * @return
     */
    public static String FormatTimeFromYearToSeconds(String dateStr){
        return simpleDateFormat.format(dateStr);
    }

    public static String FormatTimeThreadSafety(String dateStr){
        LocalDateTime localDateTime = null;
        if (dateStr.length() == 13) {
            localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(dateStr)), ZoneOffset.ofHours(8));
        }else if(dateStr.length() == 10){
            localDateTime = localDateTime.ofInstant(Instant.ofEpochSecond(Long.parseLong(dateStr)), ZoneOffset.ofHours(8));
        }
        return dateTimeFormatter.format(localDateTime);
    }

    /**
     * 根据需求设置simpleDateFormat对象
     * @param simpleDateFormat
     */
    public static void setDataFormat(SimpleDateFormat simpleDateFormat) {
        TimeParseUtil.simpleDateFormat = simpleDateFormat;
    }

    public static void main(String[] args) {
        System.out.println(FormatTimeThreadSafety("1625227746000"));
        System.out.println(parseTimeThreadSafety("2021-07-02 20:09:06"));
    }
}
