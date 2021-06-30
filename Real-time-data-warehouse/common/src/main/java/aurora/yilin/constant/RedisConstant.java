package aurora.yilin.constant;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/6/30
 */
public enum RedisConstant {
    //redis.host=hadoop122
    REDIS_HOSTNAME_LIST("redis.host.list"),
    //redis.port
    REDIS_PORT("redis.port"),
    //
    REDIS_AGREEMENT("redis.agreement"),
    //redis.module
    REDIS_MODULE("redis.module"),
    STANDALONE("standalone"),
    CLUSTER("cluster");


    private String value;


    RedisConstant(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
