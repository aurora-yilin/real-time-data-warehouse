package aurora.yilin.realtime.app.dwd;

import aurora.yilin.realtime.constant.CommonConstant;
import aurora.yilin.realtime.utils.GetResource;
import aurora.yilin.utils.KafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.netty4.io.netty.util.internal.ObjectUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Objects;
import java.util.Properties;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/6/26
 */
public class LogOperationApp {

    private static final Logger log = LoggerFactory.getLogger(LogOperationApp.class);

    public static void main(String[] args) throws Exception {

        Properties applicationPro = GetResource.getApplicationPro();

        String sourceTopic = applicationPro.getProperty(CommonConstant.ODS_BASE_LOG_TOPIC.getValue());
        String groupId = applicationPro.getProperty(CommonConstant.ODS_BASE_LOG_CONSUMER_GROUP.getValue());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /*System.setProperty("HADOOP_USER_NAME", "xxx");*/
        /*env.enableCheckpointing(Long.parseLong(PropertiesAnalysisUtil.getInfoBykeyFromPro(FlinkConstant.FLINK_CHECKPOINTING.getValue())));
        env.getCheckpointConfig().setCheckpointTimeout(Long.parseLong(PropertiesAnalysisUtil.getInfoBykeyFromPro(FlinkConstant.FLINK_CHECKPOINT_TIMEOUT.getValue())));
        env.setStateBackend(new FsStateBackend(PropertiesAnalysisUtil.getInfoBykeyFromPro(FlinkConstant.FLINK_STATE_BACKEND_FLINK_CDC.getValue())));*/

        //??????????????????json?????????????????????
        OutputTag<String> dirtyData = new OutputTag<String>(
                applicationPro.getProperty(CommonConstant.DIRTY_DATA_OUTPUTTAG.getValue())
        ) {
        };

        /**
         * ???????????????
         */
        SingleOutputStreamOperator<JSONObject> parseJsonString = env
                .addSource(KafkaUtil.getKafkaSource(sourceTopic, groupId))
                .process(new ProcessFunction<String, JSONObject>() {

                    @Override
                    public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(s);
                            collector.collect(jsonObject);
                        } catch (Exception e) {
                            log.info("parse json string faild");
                            context.output(dirtyData, s);
                        }
                    }
                });

        /*//????????????
        parseJsonString.print("normal");
        parseJsonString.getSideOutput(dirtyData).print("dirtyData");*/

        /**
         * ????????????????????????
         */
        SingleOutputStreamOperator<JSONObject> checkUserStatus = parseJsonString
                .keyBy((tempJsonObject) -> {
                    return tempJsonObject.getJSONObject("common").getString("mid");
                })
                .map(new RichMapFunction<JSONObject, JSONObject>() {
                    ValueState<Boolean> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        state = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-new", Boolean.class));
                    }

                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        String is_new = value.getJSONObject("common").getString("is_new");
                        //????????????????????????????????????,????????????????????????????????????????????????,false????????????true????????????
                        if ("1".equals(is_new)) {
                            //??????????????????????????????????????????????????????????????????jsonObject??????is_new??????
                            if (!Objects.isNull(state.value())) {
                                value.getJSONObject("common").put("is_new", 0);
                            }
                            state.update(false);
                        }
                        return value;
                    }
                });
        /**
         * ???log????????????????????????????????????????????????????????????????????????????????????????????????
         */
        OutputTag<String> startOutputTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayOutPutTag = new OutputTag<String>("display") {
        };

        SingleOutputStreamOperator<String> shuntOperation = checkUserStatus
                .process(new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {
                        String start = jsonObject.getString("start");
                        //?????????????????????????????????????????????????????????????????????
                        if (Objects.nonNull(start) && start.length() > 0) {
                            context.output(startOutputTag, jsonObject.toJSONString());
                        } else {
                            String pageId = jsonObject.getJSONObject("page").getString("page_id");
                            //???displays???jsonObject??????????????????????????????????????????JsonObject????????????display???????????????
                            JSONArray displays = jsonObject.getJSONArray("displays");
                            if (Objects.nonNull(displays) && displays.size() > 0) {
                                for (Iterator iterator = displays.iterator(); iterator.hasNext(); ) {
                                    JSONObject display = (JSONObject) iterator.next();
                                    display.put("page_id", pageId);

                                    context.output(displayOutPutTag, display.toJSONString());
                                }
                            }
                            //???jsonObject????????????
                            collector.collect(jsonObject.toJSONString());
                        }
                    }
                });

        DataStream<String> startOutput = shuntOperation.getSideOutput(startOutputTag);
        DataStream<String> displayOutput = shuntOperation.getSideOutput(displayOutPutTag);

        startOutput.addSink(KafkaUtil.getKafkaSink(applicationPro.getProperty(CommonConstant.DWD_START_LOG.getValue())));
        displayOutput.addSink(KafkaUtil.getKafkaSink(applicationPro.getProperty(CommonConstant.DWD_DISPLAY_LOG.getValue())));
        shuntOperation.addSink(KafkaUtil.getKafkaSink(applicationPro.getProperty(CommonConstant.DWD_PAGE_LOG.getValue())));


        env.execute();
    }
}
