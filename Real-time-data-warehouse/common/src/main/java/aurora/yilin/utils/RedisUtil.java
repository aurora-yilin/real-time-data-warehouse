package aurora.yilin.utils;

import aurora.yilin.constant.RedisConstant;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/6/30
 */
public class RedisUtil {
    private static StatefulRedisConnection<String, String> standaloneConnection;
    private static StatefulRedisClusterConnection<String, String> clusterConnection;

    static {
        String redisModule = PropertiesAnalysisUtil.getInfoBykeyFromPro(RedisConstant.REDIS_MODULE.getValue());
        if (RedisConstant.STANDALONE.getValue().equals(redisModule)) {
            getRedisStandalineEnv();
        } else if (RedisConstant.CLUSTER.getValue().equals(redisModule)) {
            getRedisClusterEnv();
        }
    }

    private static void getRedisClusterEnv() {
        ArrayList<RedisURI> redisHostList = new ArrayList<>();

        System.out.println(redisHostList.toString());
        Arrays.stream(PropertiesAnalysisUtil
                .getInfoBykeyFromPro(RedisConstant.REDIS_HOSTNAME_LIST.getValue())
                .split(","))
                .forEach(temp -> redisHostList.add(
                        RedisURI.create(
                                PropertiesAnalysisUtil.getInfoBykeyFromPro(RedisConstant.REDIS_AGREEMENT.getValue()) +
                                        "://" + temp + ":" +
                                        PropertiesAnalysisUtil.getInfoBykeyFromPro(RedisConstant.REDIS_PORT.getValue()))));

        RedisClusterClient redisClusterClient = RedisClusterClient.create(redisHostList);
        clusterConnection = redisClusterClient.connect();
    }

    private static void getRedisStandalineEnv() {
        RedisClient redisClient = RedisClient.create(RedisURI.create(
                PropertiesAnalysisUtil
                        .getInfoBykeyFromPro(RedisConstant.REDIS_HOSTNAME_LIST.getValue()).split(",")[0],
                Integer.parseInt(PropertiesAnalysisUtil.getInfoBykeyFromPro(RedisConstant.REDIS_PORT.getValue()))
        ));
        standaloneConnection = redisClient.connect();
    }

    public static RedisAdvancedClusterCommands<String, String> getRedisClusterSync() {
        RedisAdvancedClusterCommands<String, String> sync = clusterConnection.sync();
        return sync;
    }

    public static RedisAdvancedClusterAsyncCommands<String, String> getRedisClusterASync() {
        RedisAdvancedClusterAsyncCommands<String, String> async = clusterConnection.async();
        return async;
    }

    public static RedisCommands<String, String> getRedisStandaloneSync() {
        RedisCommands<String, String> sync = standaloneConnection.sync();
        return sync;
    }

    public static RedisAsyncCommands<String, String> getRedisStandaloneASync() {
        RedisAsyncCommands<String, String> async = standaloneConnection.async();
        return async;
    }
}
