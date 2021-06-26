package aurora.yilin.constant;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/6/25
 */
public enum KafkaConstant {

    //bootstrap.servers
    BOOTSTRAP_SERVERS("bootstrap.servers");


    private String value;


    KafkaConstant(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
