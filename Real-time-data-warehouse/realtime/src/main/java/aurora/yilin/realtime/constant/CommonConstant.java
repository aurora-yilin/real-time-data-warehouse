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
    PHOENIX_SERVER("phoenix.server"),


    //dwm_unique_visit
    DWM_UNIQUE_VISIT_TOPIC("dwm.unique.visit.topic"),
    //unique.visit.app.consumer.groupId
    UNIQUE_VISIT_APP_CONSUMER_GROUPID("unique.visit.app.consumer.groupId"),


    //dwm.bounce.rate.topic=dwm_user_jump_detail
    DWM_BOUNCE_RATE_TOPIC("dwm.bounce.rate.topic"),
    //dwm.bounce.rate.consumer.groupId=bounce_rate_app_consumer
    DWM_BOUNCE_RATE_CONSUMER_GROUPID("dwm.bounce.rate.consumer.groupId"),


//    order.info.topic=dwd_order_info
    ORDER_INFO_TOPIC("order.info.topic"),
//    order.detail.topic=dwd_order_detail
    ORDER_DETAIL_TOPIC("order.detail.topic"),
//    order.wide.topic=dwm_order_wide
    ORDER_WIDE_TOPIC("order.wide.topic"),
//    dwm.order.wide.app.consumer.groupId=order_wide_group
    DWM_ORDER_WIDE_APP_CONSUMER_GROUPID("dwm.order.wide.app.consumer.groupId"),


    //dimension
    //user.info.dimension
    USER_INFO_DIMENSION("user.info.dimension"),


//    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/default";
    CLICKHOUSE_URL("clickhouse.url"),
//    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
    CLICKHOUSE_DRIVER("clickhouse.driver");

    private String value;


    CommonConstant(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
