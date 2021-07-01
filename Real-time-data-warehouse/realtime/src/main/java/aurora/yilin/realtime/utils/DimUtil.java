package aurora.yilin.realtime.utils;

import com.alibaba.fastjson.JSONObject;

import java.sql.Connection;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/6/30
 */
public class DimUtil {

    public static JSONObject getDimInfo(Connection connection,String tableName,String id){
        // TODO: 2021/7/1 三级缓存未命中之后读hbase中数据
        return null;
    }


}
