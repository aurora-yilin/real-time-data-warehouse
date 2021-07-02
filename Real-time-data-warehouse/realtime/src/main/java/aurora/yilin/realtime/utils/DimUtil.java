package aurora.yilin.realtime.utils;

import aurora.yilin.realtime.constant.CommonConstant;
import aurora.yilin.utils.JdbcUtil;
import aurora.yilin.utils.LinkedHashLRUCacheUtil;
import aurora.yilin.utils.RedisUtil;
import com.alibaba.fastjson.JSONObject;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.concurrent.ExecutionException;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/6/30
 */
public class DimUtil {
    private static final Logger log = LoggerFactory.getLogger(DimUtil.class);

    private static LinkedHashLRUCacheUtil<String,String> linkedHashLRUCache = LinkedHashLRUCacheUtil.LRUCacheSingle.LRU_CACHE.getLinkedHashLRUCache();
    private static RedisAsyncCommands<String, String> redisStandaloneASync = RedisUtil.getRedisStandaloneASync();

    public static JSONObject getDimInfo(Connection connection,String tableName,String id){
        String Key = "DIM:" + tableName + ":" + id;
        //三级缓存
        try {
            if (linkedHashLRUCache.containsKey(Key)) {
                return JSONObject.parseObject(linkedHashLRUCache.get(Key));
            } else if (redisStandaloneASync.exists(Key).get() == 1) {
                return JSONObject.parseObject(redisStandaloneASync.get(Key).get());
            } else {
                String querySql = "select * from " +
                        GetResource.getApplicationPro().getProperty(CommonConstant.HBASE_SCHEMA.getValue()) +
                        "." + tableName + " where id='" + id + "'";
                JSONObject resultJsonObject = JdbcUtil.query(querySql, connection, JSONObject.class, false).get(0);
                linkedHashLRUCache.put(Key,resultJsonObject.toJSONString());
                redisStandaloneASync.set(Key,resultJsonObject.toJSONString());
                return resultJsonObject;
            }
        }catch (Exception e){
            log.error(e.getMessage());
        }
        return new JSONObject();
    }

    public static void delRedisDimInfo(String tableName, String id) {

        //拼接RedisKey
        String Key = "DIM:" + tableName + ":" + id;

        //删除数据
        linkedHashLRUCache.remove(Key);
        redisStandaloneASync.del(Key);

    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.out.println(redisStandaloneASync.exists("111").get()==1);
    }


}
