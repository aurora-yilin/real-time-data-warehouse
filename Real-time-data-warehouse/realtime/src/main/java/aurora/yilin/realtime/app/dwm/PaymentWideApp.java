package aurora.yilin.realtime.app.dwm;

import aurora.yilin.realtime.bean.OrderWide;
import aurora.yilin.realtime.bean.PaymentInfo;
import aurora.yilin.realtime.bean.PaymentWide;
import aurora.yilin.utils.KafkaUtil;
import aurora.yilin.utils.TimeParseUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/7/2
 */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.读取Kafka主题数据并转换为JavaBean对象
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        SingleOutputStreamOperator<PaymentInfo> paymentKafkaDS = env.addSource(KafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId))
                .map(line -> JSONObject.parseObject(line, PaymentInfo.class));
        SingleOutputStreamOperator<OrderWide> orderWideKafkaDS = env.addSource(KafkaUtil.getKafkaSource(orderWideSourceTopic, groupId))
                .map(line -> JSONObject.parseObject(line, OrderWide.class));

        //TODO 3.提取时间戳生成WaterMark
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentKafkaDS.assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                    @Override
                    public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                        return TimeParseUtil.parseTimeThreadSafety(element.getCreate_time());
                    }
                }));
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideKafkaDS.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                    @Override
                    public long extractTimestamp(OrderWide element, long recordTimestamp) {
                        return TimeParseUtil.parseTimeThreadSafety(element.getCreate_time());
                    }
                }));

        //TODO 4.双流JOIN
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoDS.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideDS.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.seconds(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });

        //TODO 5.将数据写入Kafka
        paymentWideDS.print(">>>>>>>");
        paymentWideDS
                .map(JSONObject::toJSONString)
                .addSink(KafkaUtil.getKafkaSink(paymentWideSinkTopic));

        //TODO 6.执行
        env.execute();
    }
}
