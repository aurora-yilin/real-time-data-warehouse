package aurora.yilin.realtime.constant;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/6/26
 */
public enum CommonConstant {
    //ods.base.log.topic
    ODS_BASE_LOG_TOPIC("ods.base.log.topic"),
    //ods.base.log.consumer.groupId
    ODS_BASE_LOG_CONSUMER_GROUP("ods.base.log.consumer.groupId"),
    //dirtyData.OutputTag
    DIRTY_DATA_OUTPUTTAG("dirtyData.OutputTag"),
    //dwd.start.log
    DWD_START_LOG("dwd.start.log"),
    //dwd.display.log
    DWD_DISPLAY_LOG("dwd.display.log"),
    //dwd.page.log
    DWD_PAGE_LOG("dwd.page.log"),
    //ods.base.log.topic
    ODS_BASE_DB_TOPIC("ods.base.db.topic"),
    //ods.base.log.consumer.groupId
    ODS_BASE_DB_CONSUMER_GROUP("ods.base.db.consumer.groupId"),
    //mysql.databseList=gmall2021
    MYSQL_DATABASE_LIST("mysql.database.list"),
    //SINK_TYPE_HBASE
    SINK_TYPE_HBASE("sink.type.hbase"),
    //SINK_TYPE_KAFKA
    SINK_TYPE_KAFKA("sink.type.kafka"),
    //SINK_TYPE_CK
    SINK_TYPE_CK("sink.type.ck"),

    //phoenix库名
    HBASE_SCHEMA("hbase.schema"),
    //phoenix驱动
    PHOENIX_DRIVER("phoenix.driver"),
    //phoenix连接参数
    PHOENIX_SERVER("phoenix.server");

    private String value;


    CommonConstant(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
