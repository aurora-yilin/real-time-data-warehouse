package aurora.yilin.realtime.app.dwm;

import aurora.yilin.realtime.bean.OrderDetail;
import aurora.yilin.realtime.bean.OrderInfo;
import aurora.yilin.realtime.bean.OrderWide;
import aurora.yilin.realtime.constant.CommonConstant;
import aurora.yilin.realtime.utils.GetResource;
import aurora.yilin.realtime.utils.func.DimAsyncFunction;
import aurora.yilin.utils.KafkaUtil;
import aurora.yilin.utils.TimeParseUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceTypeInfoRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/6/29
 */
public class OrderWideApp {
    private static final Logger log = LoggerFactory.getLogger(OrderWideApp.class);

    public static void main(String[] args) throws Exception {

        String orderInfoSourceTopic = GetResource.getApplicationPro().getProperty(CommonConstant.ORDER_INFO_TOPIC.getValue());
        String orderDetailSourceTopic = GetResource.getApplicationPro().getProperty(CommonConstant.ORDER_DETAIL_TOPIC.getValue());
        String orderWideSinkTopic = GetResource.getApplicationPro().getProperty(CommonConstant.ORDER_WIDE_TOPIC.getValue());
        String groupId = GetResource.getApplicationPro().getProperty(CommonConstant.DWM_ORDER_WIDE_APP_CONSUMER_GROUPID.getValue());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /*System.setProperty("HADOOP_USER_NAME", "xxx");*/
        /*env.enableCheckpointing(Long.parseLong(PropertiesAnalysisUtil.getInfoBykeyFromPro(FlinkConstant.FLINK_CHECKPOINTING.getValue())));
        env.getCheckpointConfig().setCheckpointTimeout(Long.parseLong(PropertiesAnalysisUtil.getInfoBykeyFromPro(FlinkConstant.FLINK_CHECKPOINT_TIMEOUT.getValue())));
        env.setStateBackend(new FsStateBackend(PropertiesAnalysisUtil.getInfoBykeyFromPro(FlinkConstant.FLINK_STATE_BACKEND_FLINK_CDC.getValue())));*/

        SingleOutputStreamOperator<OrderInfo> parseOrderInfoJsonObject = env.addSource(KafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId))
                .map(data -> {
                    OrderInfo tempOrderInfo = JSONObject.parseObject(data, OrderInfo.class);
                    String[] tempDateInfo = tempOrderInfo.getCreate_time().split(" ");
                    if (tempDateInfo.length == 2) {
                        tempOrderInfo.setCreate_date(tempDateInfo[0]);
                        tempOrderInfo.setCreate_hour(tempDateInfo[1].split(":")[0]);
                    } else {
                        log.error("Time format exception");
                        throw new RuntimeException("Time format exception");
                    }
                    tempOrderInfo.setCreate_ts(TimeParseUtil.parseTimeFromYearToSeconds(tempOrderInfo.getCreate_time()));
                    return tempOrderInfo;
                });

        SingleOutputStreamOperator<OrderDetail> parseOrderDetailJsonObject = env.addSource(KafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId))
                .map(data -> {
                    OrderDetail orderDetail = JSONObject.parseObject(data, OrderDetail.class);
                    String create_time = orderDetail.getCreate_time();
                    orderDetail.setCreate_ts(TimeParseUtil.parseTimeFromYearToSeconds(create_time));
                    return orderDetail;
                });
        SingleOutputStreamOperator<OrderInfo> orderInfoAddWatermark = parseOrderInfoJsonObject
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderInfo>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                                    @Override
                                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                                        return element.getCreate_ts();
                                    }
                                })
                );
        SingleOutputStreamOperator<OrderDetail> orderDetailAddWatermark = parseOrderDetailJsonObject
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderDetail>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                                    @Override
                                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                                        return element.getCreate_ts();
                                    }
                                })
                );
        //通过intervaljoin将两个流join到一起
        SingleOutputStreamOperator<OrderWide> orderWideStream = orderInfoAddWatermark.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailAddWatermark.keyBy(OrderDetail::getOrder_id))
                //根据最大网络延迟来设置时间范围
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left, right));
                    }
                });

        AsyncDataStream.unorderedWait(orderWideStream,
                new DimAsyncFunction<OrderWide>(GetResource.getApplicationPro().getProperty(CommonConstant.USER_INFO_DIMENSION.getValue())) {
                    @Override
                    public String getKey(OrderWide input) {
                        return null;
                    }

                    @Override
                    public void join(OrderWide input, JSONObject dimInfo) {

                    }
                },
                60, TimeUnit.SECONDS,9);

        env.execute();

    }
}
