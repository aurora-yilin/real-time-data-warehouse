package aurora.yilin.dbserver.utils;

import aurora.yilin.utils.PropertiesAnalysisUtil;

import java.util.Properties;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/6/26
 */
public class GetResource {
    public static Properties getApplicationPro(){
        return PropertiesAnalysisUtil.getProperties("application.properties");
    }
}
