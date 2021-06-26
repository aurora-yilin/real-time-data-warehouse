package aurora.yilin.realtime.app.dwd;

import aurora.yilin.utils.KafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        String sourceTopic = "ods_base_log";
        String groupId = "ods_base_log_210108";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //解析失败json字符串侧输出流
        OutputTag<String> dirtyData = new OutputTag<String>("DirtyData") {
        };

        /**
         * 解析字符串
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

        /*//测试代码
        parseJsonString.print("normal");
        parseJsonString.getSideOutput(dirtyData).print("dirtyData");*/

        /**
         * 校验用户身份状态
         */
        parseJsonString
                .keyBy((tempJsonObject)->{
                    return tempJsonObject.getJSONObject("common").getString("mid");
                })
                .map(new RichMapFunction<JSONObject,JSONObject>() {
                    ValueState<Boolean> state;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        state = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-new", Boolean.class));
                    }

                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        return null;
                    }
                });

        env.execute();
    }
}
