package aurora.yilin.constant;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/6/25
 */
public enum  FlinkConstant {
    //flink.checkPointing
    FLINK_CHECKPOINTING("flink.checkPoint.time"),

    //flink.checkPoint.timeout
    FLINK_CHECKPOINT_TIMEOUT("flink.checkPoint.timeout"),

    //flink.stateBackend.flinkCDC
    FLINK_STATE_BACKEND_FLINK_CDC("flink.state.Backend.flink.CDC");

    private String value;


    FlinkConstant(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
