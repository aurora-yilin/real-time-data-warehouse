package aurora.yilin.Constant;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/6/25
 */
public enum KafkaConstant {

    //bootstrap.servers
    BOOTSTRAP_SERVERS("bootstrap.servers"),

    //ods.db.topic=ods_base_db
    ODS_DB_TOPIC("ods.db.topic"),

    //ods.log.topic=ods_base_log
    OSD_LOG_TOPIC("ods.log.topic");


    private String value;


    KafkaConstant(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
