package aurora.yilin.realtime.app.dwm;

import aurora.yilin.realtime.constant.CommonConstant;
import aurora.yilin.realtime.utils.GetResource;
import aurora.yilin.utils.KafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @Description 用户跳出率(用户刚进入页面就跳出视为一次跳出)
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/6/29
 */
public class BounceRateApp {

    private static final Logger log = LoggerFactory.getLogger(BounceRateApp.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /*System.setProperty("HADOOP_USER_NAME", "xxx");*/
        /*env.enableCheckpointing(Long.parseLong(PropertiesAnalysisUtil.getInfoBykeyFromPro(FlinkConstant.FLINK_CHECKPOINTING.getValue())));
        env.getCheckpointConfig().setCheckpointTimeout(Long.parseLong(PropertiesAnalysisUtil.getInfoBykeyFromPro(FlinkConstant.FLINK_CHECKPOINT_TIMEOUT.getValue())));
        env.setStateBackend(new FsStateBackend(PropertiesAnalysisUtil.getInfoBykeyFromPro(FlinkConstant.FLINK_STATE_BACKEND_FLINK_CDC.getValue())));*/

        String sourceTopicId = GetResource.getApplicationPro().getProperty(CommonConstant.DWD_PAGE_LOG.getValue());
        String sinkTopicId = GetResource.getApplicationPro().getProperty(CommonConstant.DWM_BOUNCE_RATE_TOPIC.getValue());
        String consuemrGroup = GetResource.getApplicationPro().getProperty(CommonConstant.DWM_BOUNCE_RATE_CONSUMER_GROUPID.getValue());

        //将kafka中读取出的数据转换为json对象并对其根据mid进行keyby分区操作
        KeyedStream<JSONObject, String> keyByMidJsonDataStream = env.addSource(KafkaUtil.getKafkaSource(sourceTopicId, consuemrGroup))
                .flatMap(new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                        try {
                            out.collect(JSON.parseObject(value));
                        } catch (JSONException je) {
                            log.warn("BounceRateApp find Irregular json string");
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1L))
                                .withTimestampAssigner(new WatermarkStrategy<JSONObject>() {
                                    @Override
                                    public WatermarkGenerator<JSONObject> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                                        return new WatermarkGenerator<JSONObject>() {
                                            @Override
                                            public void onEvent(JSONObject event, long eventTimestamp, WatermarkOutput output) {
                                                output.emitWatermark(new Watermark(event.getLong("ts")));
                                            }

                                            @Override
                                            public void onPeriodicEmit(WatermarkOutput output) {

                                            }
                                        };
                                    }
                                })
                )
                .keyBy(temp -> {
                    return temp.getJSONObject("common").getString("mid");
                });

        //通过FlinkCEP来进行跳出数据的过滤动作
        Pattern<JSONObject, JSONObject> bounceRatePattern = Pattern
                .<JSONObject>begin("start")
                .where(new IterativeCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value, Context<JSONObject> ctx) throws Exception {
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return Objects.isNull(lastPageId) && lastPageId.length() <= 0;
                    }
                }).next("next")
                .where(new IterativeCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value, Context<JSONObject> ctx) throws Exception {
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return Objects.isNull(lastPageId) && lastPageId.length() <= 0;
                    }
                }).within(Time.seconds(10));

        OutputTag<JSONObject> timeOutOutputTag = new OutputTag<JSONObject>("timedOutPartialMatchesTag"){};
        PatternStream<JSONObject> jsonObjectPatternStream = CEP.pattern(keyByMidJsonDataStream, bounceRatePattern);

        SingleOutputStreamOperator<JSONObject> result = jsonObjectPatternStream.select(timeOutOutputTag, new PatternTimeoutFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp) throws Exception {
                return pattern.get("start").get(0);
            }
        }, new PatternSelectFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject select(Map<String, List<JSONObject>> pattern) throws Exception {
                return pattern.get("start").get(0);
            }
        });

        DataStream<JSONObject> sideOutput = result.getSideOutput(timeOutOutputTag);
        result.union(sideOutput).map(JSONAware::toJSONString).addSink(KafkaUtil.getKafkaSink(sinkTopicId));

        env.execute();

    }
}
