package aurora.yilin.realtime.app.dwm;

import aurora.yilin.realtime.constant.CommonConstant;
import aurora.yilin.realtime.utils.GetResource;
import aurora.yilin.utils.KafkaUtil;
import aurora.yilin.utils.TimeParseUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.sun.xml.internal.fastinfoset.util.ValueArray;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Objects;

/**
 * @Description 用户日活指标
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/6/28
 */
public class DAUApp {
    private static final Logger log = LoggerFactory.getLogger(DAUApp.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /*System.setProperty("HADOOP_USER_NAME", "xxx");*/
        /*env.enableCheckpointing(Long.parseLong(PropertiesAnalysisUtil.getInfoBykeyFromPro(FlinkConstant.FLINK_CHECKPOINTING.getValue())));
        env.getCheckpointConfig().setCheckpointTimeout(Long.parseLong(PropertiesAnalysisUtil.getInfoBykeyFromPro(FlinkConstant.FLINK_CHECKPOINT_TIMEOUT.getValue())));
        env.setStateBackend(new FsStateBackend(PropertiesAnalysisUtil.getInfoBykeyFromPro(FlinkConstant.FLINK_STATE_BACKEND_FLINK_CDC.getValue())));*/

        String sourceTopic = GetResource.getApplicationPro().getProperty(CommonConstant.DWD_PAGE_LOG.getValue());
        String consumerGroupId = GetResource.getApplicationPro().getProperty(CommonConstant.UNIQUE_VISIT_APP_CONSUMER_GROUPID.getValue());
        String sinkTopic = GetResource.getApplicationPro().getProperty(CommonConstant.DWM_UNIQUE_VISIT_TOPIC.getValue());

        env.addSource(KafkaUtil.getKafkaSource(sourceTopic,consumerGroupId))
                .flatMap(new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                        try{
                            JSONObject tempJsonObject = JSON.parseObject(value);
                            out.collect(tempJsonObject);
                        }catch (JSONException je){
                            log.warn("DAUAPP find Irregular json string");
                        }
                    }
                })
                .keyBy(tempJsonObject -> {
                    return tempJsonObject.getJSONObject("common").getString("mid");
                })
                .filter(new RichFilterFunction<JSONObject>() {

                    ValueState<String> lastLoginDate;
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        //创建状态描述器
                        ValueStateDescriptor<String> lastLoginDateValueDescriptor = new ValueStateDescriptor<>("lastLoginDate", String.class);

                        //为状态描述器配置ttl
                        StateTtlConfig stateTtlConfig = StateTtlConfig
                                .newBuilder(Time.days(1))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .build();
                        lastLoginDateValueDescriptor.enableTimeToLive(stateTtlConfig);

                        this.lastLoginDate = getRuntimeContext().getState(lastLoginDateValueDescriptor);
                    }

                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        if (lastPageId == null || lastPageId.length()<=0) {
                            lastLoginDate.update(TimeParseUtil.FormatTimeThreadSafety(value.getString("ts")));
                            return true;
                        }else {
                            if (Objects.isNull(lastLoginDate.value())){
                                lastLoginDate.update(TimeParseUtil.FormatTimeThreadSafety(value.getString("ts")));
                                return true;
                            }else{
                                if (lastLoginDate.value().equals(TimeParseUtil.FormatTimeThreadSafety(value.getString("ts")))) {
                                    return false;
                                }else{
                                    return true;
                                }
                            }
                        }
                    }
                })
                .map(temp -> temp.toJSONString())
                .addSink(KafkaUtil.getKafkaSink(sinkTopic));

        env.execute();
    }
}
