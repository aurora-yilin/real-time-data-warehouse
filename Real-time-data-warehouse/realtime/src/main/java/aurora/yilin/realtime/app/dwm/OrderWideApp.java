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

import java.text.SimpleDateFormat;
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
                    tempOrderInfo.setCreate_ts(TimeParseUtil.parseTimeThreadSafety(tempOrderInfo.getCreate_time()));
                    return tempOrderInfo;
                });

        SingleOutputStreamOperator<OrderDetail> parseOrderDetailJsonObject = env.addSource(KafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId))
                .map(data -> {
                    OrderDetail orderDetail = JSONObject.parseObject(data, OrderDetail.class);
                    String create_time = orderDetail.getCreate_time();
                    orderDetail.setCreate_ts(TimeParseUtil.parseTimeThreadSafety(create_time));
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

        SingleOutputStreamOperator<OrderWide> orderWideDS = AsyncDataStream.unorderedWait(orderWideStream,
                new DimAsyncFunction<OrderWide>(GetResource.getApplicationPro().getProperty(CommonConstant.USER_INFO_DIMENSION.getValue())) {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide input, JSONObject dimInfo) {
                        //补充用户性别
                        String gender = dimInfo.getString("GENDER");
                        input.setUser_gender(gender);

                        //补充用户年龄
                        String birthday = dimInfo.getString("BIRTHDAY");
                        long ts = TimeParseUtil.parseTimeThreadSafety(birthday);

                        Long age = (System.currentTimeMillis() - ts) / (1000 * 60 * 60 * 24 * 365L);
                        input.setUser_age(new Integer(age.toString()));
                    }
                },
                60, TimeUnit.SECONDS, 9);

        //TODO 5.2关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo){

                        //提取维度数据
                        String name = dimInfo.getString("NAME");
                        String area_code = dimInfo.getString("AREA_CODE");
                        String iso_code = dimInfo.getString("ISO_CODE");
                        String code2 = dimInfo.getString("ISO_3166_2");

                        //将数据补充进OrderWide
                        orderWide.setProvince_name(name);
                        orderWide.setProvince_area_code(area_code);
                        orderWide.setProvince_iso_code(iso_code);
                        orderWide.setProvince_3166_2_code(code2);

                    }
                }, 60,
                TimeUnit.SECONDS);

        //TODO 5.3关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(orderWideWithProvinceDS,
                new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSku_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo){

                        //提取数据
                        Long spu_id = dimInfo.getLong("SPU_ID");
                        Long tm_id = dimInfo.getLong("TM_ID");
                        Long category3_id = dimInfo.getLong("CATEGORY3_ID");

                        //赋值
                        orderWide.setSpu_id(spu_id);
                        orderWide.setTm_id(tm_id);
                        orderWide.setCategory3_id(category3_id);

                    }
                }, 60,
                TimeUnit.SECONDS);

        //TODO 5.4关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(orderWideWithSkuDS,
                new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSpu_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo){
                        //获取数据
                        String spu_name = dimInfo.getString("SPU_NAME");

                        //赋值
                        orderWide.setSpu_name(spu_name);
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 5.5关联TradeMark维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject){
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 5.6关联Category维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithTmDS, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject){
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 6.将数据写入Kafka
        orderWideWithCategory3DS.print("关联所有维度之后>>>>");
        orderWideWithCategory3DS
                .map(JSONObject::toJSONString)
                .addSink(KafkaUtil.getKafkaSink(orderWideSinkTopic));

        env.execute();

    }
}
