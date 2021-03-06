package aurora.yilin.realtime.app.dwd;

import aurora.yilin.constant.MySqlConstant;
import aurora.yilin.realtime.app.dwd.func.TableProcessFunction;
import aurora.yilin.realtime.bean.TableProcess;
import aurora.yilin.realtime.constant.CommonConstant;
import aurora.yilin.realtime.utils.DimUtil;
import aurora.yilin.realtime.utils.GetResource;
import aurora.yilin.utils.KafkaUtil;
import aurora.yilin.utils.PropertiesAnalysisUtil;
import aurora.yilin.utils.func.DebeziumDeserializationSchema;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/6/27
 */
public class DbOperationApp {
    private static final Logger log = LoggerFactory.getLogger(DbOperationApp.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /*System.setProperty("HADOOP_USER_NAME", "xxx");*/
        /*env.enableCheckpointing(Long.parseLong(PropertiesAnalysisUtil.getInfoBykeyFromPro(FlinkConstant.FLINK_CHECKPOINTING.getValue())));
        env.getCheckpointConfig().setCheckpointTimeout(Long.parseLong(PropertiesAnalysisUtil.getInfoBykeyFromPro(FlinkConstant.FLINK_CHECKPOINT_TIMEOUT.getValue())));
        env.setStateBackend(new FsStateBackend(PropertiesAnalysisUtil.getInfoBykeyFromPro(FlinkConstant.FLINK_STATE_BACKEND_FLINK_CDC.getValue())));*/

        Properties applicationPro = GetResource.getApplicationPro();
        String topic = applicationPro.getProperty(CommonConstant.ODS_BASE_DB_TOPIC.getValue());
        String groupId = applicationPro.getProperty(CommonConstant.ODS_BASE_DB_CONSUMER_GROUP.getValue());

        //??????kfaka??????????????????????????????json???????????????
        SingleOutputStreamOperator<JSONObject> transformJsonObject = env
                .addSource(KafkaUtil.getKafkaSource(topic, groupId))
                .flatMap(new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String value, Collector<JSONObject> out) {
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            out.collect(jsonObject);
                        } catch (JSONException je) {
                            log.warn("dwd dbOperationApp find Irregular json string");
                        }
                    }
                });
        //??????delete?????????????????????
        SingleOutputStreamOperator<JSONObject> filterDataIsNull = transformJsonObject
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String dataString = value.getString("data");
                        if (Objects.isNull(dataString)||dataString.length()<=2) {
//                            Objects.requireNonNull(dataString);
                            return false;
                        }
                        return true;
                    }
                });

        //??????FlinkCDC???????????????mysql????????????????????????
        DebeziumSourceFunction<String> mysqlSource = MySQLSource.<String>builder()
                .hostname(PropertiesAnalysisUtil.getInfoBykeyFromPro(MySqlConstant.MYSQL_HOSTNAME.getValue()))
                .port(Integer.parseInt(PropertiesAnalysisUtil.getInfoBykeyFromPro(MySqlConstant.MYSQL_PORT.getValue())))
                .username(PropertiesAnalysisUtil.getInfoBykeyFromPro(MySqlConstant.MYSQL_USERNAME.getValue()))
                .password(PropertiesAnalysisUtil.getInfoBykeyFromPro(MySqlConstant.MYSQL_PASSWORD.getValue()))
                .databaseList(applicationPro.getProperty(CommonConstant.MYSQL_DATABASE_LIST.getValue()))
                .startupOptions(StartupOptions.initial())
                .deserializer(new DebeziumDeserializationSchema())
                .build();
        System.out.println("<<<<<<<<<<<<<<<<<<" + applicationPro.getProperty(CommonConstant.MYSQL_DATABASE_LIST.getValue()) + ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

        //??????????????????????????????
        MapStateDescriptor<String,TableProcess> CONFIGURE = new MapStateDescriptor<>("config", String.class, TableProcess.class);

        //???????????????
        BroadcastStream<String> broadcastStream = env.addSource(mysqlSource).setParallelism(1).broadcast(CONFIGURE);

        //??????hbase????????????
        OutputTag<JSONObject> hbaseOutputTag = new OutputTag<JSONObject>(applicationPro.getProperty(CommonConstant.SINK_TYPE_HBASE.getValue())) {
        };

        //??????connect?????????????????????????????????????????????????????????
        BroadcastConnectedStream<JSONObject, String> broadcastConnectedStream = filterDataIsNull.connect(broadcastStream);

        //???connect?????????????????????
        SingleOutputStreamOperator<JSONObject> process = broadcastConnectedStream.process(new TableProcessFunction(hbaseOutputTag, CONFIGURE));

        process.getSideOutput(hbaseOutputTag).addSink(new HbaseDimSink());
        process.addSink(KafkaUtil.getKafkaProducer(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<>(element.getString("sinkTable"),
                        element.getString("data").getBytes());
            }
        }));

        env.execute();

    }

    /**
     * ?????????????????????????????????????????????????????????????????????????????????
     */


    public static class HbaseDimSink extends RichSinkFunction<JSONObject> {

        //????????????
        private Connection connection;
        private Properties properties;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.properties = GetResource.getApplicationPro();

            //????????????
            Class.forName(properties.getProperty(CommonConstant.PHOENIX_DRIVER.getValue()));
            connection = DriverManager.getConnection(properties.getProperty(CommonConstant.PHOENIX_SERVER.getValue()));
        }

        //value:{"sinkTable":"dim_xxx","database":"","tableName":"","type":"","data":{"id":"1",...},"before":{"id":"1001",...}}
        //sql:upsert into yy.table_name (id,name,sex) values(xxx,xxx,xxx)
        @Override
        public void invoke(JSONObject value, Context context) throws Exception {
            PreparedStatement preparedStatement = null;
            try {
                //1.??????SQL??????
                String tableName = value.getString("sinkTable");
                JSONObject data = value.getJSONObject("data");
                String upsertSql = genUpsertSql(tableName, data);
                System.out.println(upsertSql);
                if ("update".equals(value.getString("type"))) {
                    DimUtil.delRedisDimInfo(tableName,value.getJSONObject("data").getString("id"));
                }

                //2.??????SQL
                preparedStatement = connection.prepareStatement(upsertSql.toString());

                //3.????????????????????????
                preparedStatement.execute();
                connection.commit();

            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
            }

        }

        /**
         * @param tableName ??????????????????
         * @param data      ?????????????????? ??? {"id":"1001","name":"zhangsan","sex":"male"}
         * @return "upsert into yy.table_name (id,name,sex) values('1001','zhangsan','male')"
         */
        private String genUpsertSql(String tableName, JSONObject data) {

            //??????SQL??????
            StringBuilder sql = new StringBuilder("upsert into ")
                    .append(properties.getProperty(CommonConstant.HBASE_SCHEMA.getValue()))
                    .append(".")
                    .append(tableName)
                    .append("(");

            //???????????????
            Set<String> keySet = data.keySet();
            sql.append(StringUtils.join(keySet, ","))
                    .append(") values ('");

            //???????????????
            Collection<Object> values = data.values();
            sql.append(StringUtils.join(values, "','"))
                    .append("')");

            return sql.toString();
        }
    }
}
