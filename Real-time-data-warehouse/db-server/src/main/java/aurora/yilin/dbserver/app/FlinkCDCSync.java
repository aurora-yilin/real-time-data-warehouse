package aurora.yilin.dbserver.app;

import aurora.yilin.constant.MySqlConstant;
import aurora.yilin.dbserver.constant.CommonConstant;
import aurora.yilin.dbserver.utils.GetResource;
import aurora.yilin.utils.KafkaUtil;
import aurora.yilin.utils.PropertiesAnalysisUtil;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/6/25
 */
public class FlinkCDCSync {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /*env.enableCheckpointing(Long.parseLong(PropertiesAnalysisUtil.getInfoBykeyFromPro(FlinkConstant.FLINK_CHECKPOINTING.getValue())));
        env.getCheckpointConfig().setCheckpointTimeout(Long.parseLong(PropertiesAnalysisUtil.getInfoBykeyFromPro(FlinkConstant.FLINK_CHECKPOINT_TIMEOUT.getValue())));
        env.setStateBackend(new FsStateBackend(PropertiesAnalysisUtil.getInfoBykeyFromPro(FlinkConstant.FLINK_STATE_BACKEND_FLINK_CDC.getValue())));*/

        env.setParallelism(1);

        DebeziumSourceFunction<String> mysqlSource = MySQLSource.<String>builder()
                .hostname(PropertiesAnalysisUtil.getInfoBykeyFromPro(MySqlConstant.MYSQL_HOSTNAME.getValue()))
                .port(Integer.parseInt(PropertiesAnalysisUtil.getInfoBykeyFromPro(MySqlConstant.MYSQL_PORT.getValue())))
                .username(PropertiesAnalysisUtil.getInfoBykeyFromPro(MySqlConstant.MYSQL_USERNAME.getValue()))
                .password(PropertiesAnalysisUtil.getInfoBykeyFromPro(MySqlConstant.MYSQL_PASSWORD.getValue()))
                .databaseList(PropertiesAnalysisUtil.getInfoBykeyFromPro(CommonConstant.MYSQL_DATABASE_LIST.getValue()))
                .startupOptions(StartupOptions.latest())
                .deserializer(new DebeziumDeserializationSchema())
                .build();
        env.addSource(mysqlSource).print();
        env.addSource(mysqlSource)
                .addSink(KafkaUtil.getKafkaSink(GetResource.getApplicationPro().getProperty(CommonConstant.ODS_DB_TOPIC.getValue())));

        env.execute();


    }

    public static class DebeziumDeserializationSchema implements com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema<String> {
        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
            //获取主题信息,包含着数据库和表名  mysql_binlog_source.gmall-flink.z_user_info
            String topic = sourceRecord.topic();
            String[] arr = topic.split("\\.");
            String db = arr[1];
            String tableName = arr[2];

            //获取操作类型 READ DELETE UPDATE CREATE
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);

            //获取值信息并转换为Struct类型
            Struct value = (Struct)sourceRecord.value();

            //获取变化后的数据
            Struct after = value.getStruct("after");

            //创建JSON对象用于存储数据信息
            JSONObject data = new JSONObject();
            if (after != null) {
                Schema schema = after.schema();
                for (Field field : schema.fields()) {
                    data.put(field.name(), after.get(field.name()));
                }
            }

            //创建JSON对象用于封装最终返回值数据信息
            JSONObject result = new JSONObject();
            result.put("operation", operation.toString().toLowerCase());
            result.put("data", data);
            result.put("database", db);
            result.put("table", tableName);

            //发送数据至下游
            collector.collect(result.toJSONString());

        }

        @Override
        public TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }
}
