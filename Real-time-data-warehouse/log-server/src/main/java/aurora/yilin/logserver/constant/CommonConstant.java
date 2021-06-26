package aurora.yilin.logserver.constant;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/6/26
 */
public enum CommonConstant {

    //ods.log.topic=ods_base_log
    OSD_LOG_TOPIC("ods.log.topic");


    private String value;


    CommonConstant(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
