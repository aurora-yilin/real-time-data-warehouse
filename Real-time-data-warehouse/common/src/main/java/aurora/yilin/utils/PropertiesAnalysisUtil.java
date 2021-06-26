package aurora.yilin.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/6/25
 */
public class PropertiesAnalysisUtil {

    private static final Logger log = LoggerFactory.getLogger(PropertiesAnalysisUtil.class);


    private static Properties properties;

    static {
        try {
            properties = new Properties();
            properties.load(PropertiesAnalysisUtil.class.getClassLoader().getResourceAsStream("realTime.properties"));

        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    public static String getInfoBykeyFromPro(String key){
        return properties.getProperty(key);
    }

    public static Properties getProperties(String fileName){
        Properties properties = null;
        try {
            properties = new Properties();
            properties.load(PropertiesAnalysisUtil.class.getClassLoader().getResourceAsStream(fileName));
        } catch (IOException e) {
            log.error(e.getMessage());
        }
        return properties;
    }
}
