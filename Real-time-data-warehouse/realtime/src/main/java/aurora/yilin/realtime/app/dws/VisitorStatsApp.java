package aurora.yilin.realtime.app.dws;

import aurora.yilin.realtime.bean.VisitorStats;
import aurora.yilin.realtime.utils.ClickhouseUtil;
import aurora.yilin.utils.KafkaUtil;
import aurora.yilin.utils.TimeParseUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/7/2
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //如果读取的是Kafka中数据,则需要与Kafka的分区数保持一致
        env.setParallelism(1);


        //TODO 2.读取Kafka主题创建流并转换为JSON对象
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        String groupId = "visitor_stats_app";
        SingleOutputStreamOperator<JSONObject> uvKafkaDS = env.addSource(KafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId))
                .map(JSONObject::parseObject);
        SingleOutputStreamOperator<JSONObject> ujKafkaDS = env.addSource(KafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId))
                .map(JSONObject::parseObject);
        SingleOutputStreamOperator<JSONObject> pageKafkaDS = env.addSource(KafkaUtil.getKafkaSource(pageViewSourceTopic, groupId))
                .map(JSONObject::parseObject);

        //TODO 3.处理数据,让多个流中数据类型统一

        //3.1 uvKafkaDS
        SingleOutputStreamOperator<VisitorStats> visitorStatsUvDS = uvKafkaDS.map(json -> {
            //获取公共字段
            JSONObject common = json.getJSONObject("common");

            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    json.getLong("ts"));
        });

        //3.2 ujKafkaDS
        SingleOutputStreamOperator<VisitorStats> visitorStatsUjDS = ujKafkaDS.map(json -> {
            //获取公共字段
            JSONObject common = json.getJSONObject("common");

            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 1L, 0L,
                    json.getLong("ts"));
        });

        //3.3 pageKafkaDS
        SingleOutputStreamOperator<VisitorStats> visitorStatsPageDS = pageKafkaDS.map(json -> {
            //获取公共字段
            JSONObject common = json.getJSONObject("common");

            //获取访问时间
            JSONObject page = json.getJSONObject("page");
            Long durTime = page.getLong("during_time");

            long sv = 0L;
            String last_page_id = page.getString("last_page_id");
            if (last_page_id == null) {
                sv = 1L;
            }

            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 1L, sv, 0L, durTime,
                    json.getLong("ts"));
        });

        //TODO 4.Union多个流
        DataStream<VisitorStats> unionDS = visitorStatsUvDS.union(visitorStatsUjDS,
                visitorStatsPageDS);

        //TODO 5.提取时间戳生成Watermark
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWmDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                }));

        //TODO 6.分组,开窗,聚合
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = visitorStatsWithWmDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return new Tuple4<>(value.getCh(),
                        value.getAr(),
                        value.getIs_new(),
                        value.getVc());
            }
        });
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<VisitorStats> result = windowedStream.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                return new VisitorStats("", "",
                        value1.getVc(),
                        value1.getCh(),
                        value1.getAr(),
                        value1.getIs_new(),
                        value1.getUv_ct() + value2.getUv_ct(),
                        value1.getPv_ct() + value2.getPv_ct(),
                        value1.getSv_ct() + value2.getSv_ct(),
                        value1.getUj_ct() + value2.getUj_ct(),
                        value1.getDur_sum() + value2.getDur_sum(),
                        value2.getTs());
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {

                //获取窗口的开始和结束时间
                long start = window.getStart();
                long end = window.getEnd();

                //获取聚合后的数据
                VisitorStats visitorStats = input.iterator().next();

                //补充字段
                visitorStats.setStt(TimeParseUtil.FormatTimeThreadSafety(String.valueOf(start)));
                visitorStats.setEdt(TimeParseUtil.FormatTimeThreadSafety(String.valueOf(end)));

                out.collect(visitorStats);
            }
        });

        //TODO 7.写入ClickHouse
        result.addSink(ClickhouseUtil.clickHouseJdbcSink("insert into visitor_stats_210108 values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 8.启动
        env.execute();

    }
}
