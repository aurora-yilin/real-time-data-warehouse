package aurora.yilin.realtime.app.dwd;

import aurora.yilin.constant.MySqlConstant;
import aurora.yilin.realtime.bean.TableProcess;
import aurora.yilin.realtime.constant.CommonConstant;
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

        /*System.setProperty("HADOOP_USER_NAME", "xxx");*/
        /*env.enableCheckpointing(Long.parseLong(PropertiesAnalysisUtil.getInfoBykeyFromPro(FlinkConstant.FLINK_CHECKPOINTING.getValue())));
        env.getCheckpointConfig().setCheckpointTimeout(Long.parseLong(PropertiesAnalysisUtil.getInfoBykeyFromPro(FlinkConstant.FLINK_CHECKPOINT_TIMEOUT.getValue())));
        env.setStateBackend(new FsStateBackend(PropertiesAnalysisUtil.getInfoBykeyFromPro(FlinkConstant.FLINK_STATE_BACKEND_FLINK_CDC.getValue())));*/

        Properties applicationPro = GetResource.getApplicationPro();
        String topic = applicationPro.getProperty(CommonConstant.ODS_BASE_DB_TOPIC.getValue());
        String groupId = applicationPro.getProperty(CommonConstant.ODS_BASE_DB_CONSUMER_GROUP.getValue());

        //将从kfaka中读取到的数据转换成json字符串类型
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
        //过滤delete操作类型的数据
        SingleOutputStreamOperator<JSONObject> filterDataIsNull = transformJsonObject
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String dataString = value.getString("data");
                        if (Objects.isNull(dataString) && dataString.length() <= 2) {
                            return false;
                        } else {
                            return true;
                        }
                    }
                });

        //通过FlinkCDC来实时获取mysql中对应库的修改信息
        DebeziumSourceFunction<String> mysqlSource = MySQLSource.<String>builder()
                .hostname(PropertiesAnalysisUtil.getInfoBykeyFromPro(MySqlConstant.MYSQL_HOSTNAME.getValue()))
                .port(Integer.parseInt(PropertiesAnalysisUtil.getInfoBykeyFromPro(MySqlConstant.MYSQL_PORT.getValue())))
                .username(PropertiesAnalysisUtil.getInfoBykeyFromPro(MySqlConstant.MYSQL_USERNAME.getValue()))
                .password(PropertiesAnalysisUtil.getInfoBykeyFromPro(MySqlConstant.MYSQL_PASSWORD.getValue()))
                .databaseList(PropertiesAnalysisUtil.getInfoBykeyFromPro(CommonConstant.MYSQL_DATABASE_LIST.getValue()))
                .startupOptions(StartupOptions.latest())
                .deserializer(new DebeziumDeserializationSchema())
                .build();


        MapStateDescriptor CONFIGUR = new MapStateDescriptor("config", StringSerializer.INSTANCE, StringSerializer.INSTANCE);
        BroadcastStream<String> broadcastStream = env.addSource(mysqlSource).setParallelism(1).broadcast(CONFIGUR);

        OutputTag<JSONObject> hbaseOutputTag = new OutputTag<JSONObject>(applicationPro.getProperty(CommonConstant.SINK_TYPE_HBASE.getValue()));

        //将主流与广播流合并
        BroadcastConnectedStream<JSONObject, String> broadcastConnectedStream = filterDataIsNull.connect(broadcastStream);

        SingleOutputStreamOperator<JSONObject> process = broadcastConnectedStream.process(new TableProcessFunction(hbaseOutputTag, CONFIGUR));

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

    public static class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

        private OutputTag<JSONObject> outputTag;
        private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
        private Connection connection;

        public TableProcessFunction(OutputTag<JSONObject> outputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
            this.outputTag = outputTag;
            this.mapStateDescriptor = mapStateDescriptor;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            Class.forName(GetResource.getApplicationPro().getProperty(CommonConstant.PHOENIX_DRIVER.getValue()));
            connection = DriverManager.getConnection(GetResource.getApplicationPro().getProperty(CommonConstant.PHOENIX_SERVER.getValue()));
        }

        //value:{"database":"","tableName":"","type":"","data":{"source_table":"base_trademark",...},"before":{"id":"1001",...}}
        @Override
        public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

            //将广播流中的json对象解析为TableProcess对象
            JSONObject jsonObject = JSONObject.parseObject(value);
            TableProcess tableProcess = JSON.parseObject(jsonObject.getString("data"), TableProcess.class);

            //根据解析后的tableProcess对象中的信息通过phoenix创建表
            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                checkTable(tableProcess.getSinkTable(),
                        tableProcess.getSinkColumns(),
                        tableProcess.getSinkPk(),
                        tableProcess.getSinkExtend());
            }

            //将广播流中的信息广播出去
            BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
            String key = tableProcess.getSourceTable() + "_" + tableProcess.getOperateType();
            broadcastState.put(key, tableProcess);
        }


        //在Phoenix中建表  create table if not exists yy.xx(aa varchar primary key,bb varchar)
        private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

            PreparedStatement preparedStatement = null;

            try {
                //处理主键和扩展字段,给定默认值
                if (sinkPk == null) {
                    sinkPk = "id";
                }
                if (sinkExtend == null) {
                    sinkExtend = "";
                }

                //1.拼接SQL
                StringBuilder createSql = new StringBuilder("create table if not exists ");
                createSql.append(GetResource.getApplicationPro().getProperty(CommonConstant.HBASE_SCHEMA.getValue()))
                        .append(".")
                        .append(sinkTable)
                        .append("(");

                String[] columns = sinkColumns.split(",");
                for (int i = 0; i < columns.length; i++) {

                    String column = columns[i];
                    //判断当前的列是否为主键
                    if (sinkPk.equals(column)) {
                        createSql.append(column).append(" varchar primary key ");
                    } else {
                        createSql.append(column).append(" varchar ");
                    }

                    //如果当前字段不是最后一个字段,则需要添加","
                    if (i < columns.length - 1) {
                        createSql.append(",");
                    }
                }

                //拼接扩展字段
                createSql.append(")").append(sinkExtend);

                //打印建表语句
                System.out.println(createSql.toString());

                //2.编译SQL
                preparedStatement = connection.prepareStatement(createSql.toString());

                //3.执行,建表
                preparedStatement.execute();

            } catch (SQLException e) {
                throw new RuntimeException("Phoenix创建" + sinkTable + "表失败！");
            } finally {

                if (preparedStatement != null) {
                    try {
                        preparedStatement.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        //value:{"database":"","tableName":"","type":"","data":{"id":"1",...},"before":{"id":"1001",...}}
        @Override
        public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

            //获取广播流存储的广播信息
            ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
            //根据当前数据流中数据的信息字段获取对应的广播信息
            String key = value.getString("tableName") + "_" + value.getString("type");
            TableProcess tableProcess = broadcastState.get(key);

            //该数据对应的广播信息不为null
            if (tableProcess != null) {

                //过滤掉jsonObject中不要求保留的字段的信息
                filterColumn(value.getJSONObject("data"), tableProcess.getSinkColumns());

                //给校验过的jsonObject对象添加sinkTable信息
                value.put("sinkTable", tableProcess.getSinkTable());

                //根据JsonObject对象对应的配置信息来选择对应的输出流
                if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                    //hbase维度数据写入测输出流
                    ctx.output(outputTag, value);
                } else if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                    //事实数据写入主流
                    out.collect(value);
                }

            } else {
                System.out.println("该组合Key：" + key + "不存在配置信息！");
            }

        }

        //根据配置信息过滤字段 data：{"id":"1","tm_name":"atguigu","logo":"xxx"}
        private void filterColumn(JSONObject data, String sinkColumns) {

            //拆分需要保留的字段信息并转化为list结构便于后续操作
            String[] columnsArr = sinkColumns.split(",");
            List<String> columnList = Arrays.asList(columnsArr);

            //将JsonObject中所有的字段信息转化成set结构
            Set<Map.Entry<String, Object>> entries = data.entrySet();
            //根据保留字段移除多余的字段
            entries.removeIf(next -> !columnList.contains(next.getKey()));

            /*Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> next = iterator.next();
                if (!columnList.contains(next.getKey())) {
                    iterator.remove();
                }
            }*/

        }
    }

    public static class HbaseDimSink extends RichSinkFunction<JSONObject> {

        //声明连接
        private Connection connection;
        private Properties properties;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.properties = GetResource.getApplicationPro();

            //创建连接
            Class.forName(properties.getProperty(CommonConstant.PHOENIX_DRIVER.getValue()));
            connection = DriverManager.getConnection(properties.getProperty(CommonConstant.PHOENIX_SERVER.getValue()));
        }

        //value:{"sinkTable":"dim_xxx","database":"","tableName":"","type":"","data":{"id":"1",...},"before":{"id":"1001",...}}
        //sql:upsert into yy.table_name (id,name,sex) values(xxx,xxx,xxx)
        @Override
        public void invoke(JSONObject value, Context context) throws Exception {
            PreparedStatement preparedStatement = null;
            try {
                //1.封装SQL语句
                String tableName = value.getString("sinkTable");
                JSONObject data = value.getJSONObject("data");
                String upsertSql = genUpsertSql(tableName, data);
                System.out.println(upsertSql);

                //2.编译SQL
                preparedStatement = connection.prepareStatement(upsertSql.toString());

                //3.执行插入数据操作
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
         * @param tableName 维度数据表名
         * @param data      待写入的数据 ： {"id":"1001","name":"zhangsan","sex":"male"}
         * @return "upsert into yy.table_name (id,name,sex) values('1001','zhangsan','male')"
         */
        private String genUpsertSql(String tableName, JSONObject data) {

            //构建SQL语句
            StringBuilder sql = new StringBuilder("upsert into ")
                    .append(properties.getProperty(CommonConstant.HBASE_SCHEMA.getValue()))
                    .append(".")
                    .append(tableName)
                    .append("(");

            //拼接列信息
            Set<String> keySet = data.keySet();
            sql.append(StringUtils.join(keySet, ","))
                    .append(") values ('");

            //拼接值信息
            Collection<Object> values = data.values();
            sql.append(StringUtils.join(values, "','"))
                    .append("')");

            return sql.toString();
        }
    }
}
