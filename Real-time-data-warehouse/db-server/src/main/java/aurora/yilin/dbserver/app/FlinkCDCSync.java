package aurora.yilin.dbserver.app;

import aurora.yilin.constant.MySqlConstant;
import aurora.yilin.dbserver.constant.CommonConstant;
import aurora.yilin.dbserver.utils.GetResource;
import aurora.yilin.utils.func.DebeziumDeserializationSchema;
import aurora.yilin.utils.KafkaUtil;
import aurora.yilin.utils.PropertiesAnalysisUtil;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @Description 通过FlinkCDC将mysql的binlog中的数据进行监听并发往kafka
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/6/25
 */
public class FlinkCDCSync {
    public static void main(String[] args) throws Exception {
        /*System.setProperty("HADOOP_USER_NAME", "xxx");*/

        Properties applicationPro = GetResource.getApplicationPro();
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
                .databaseList(applicationPro.getProperty(CommonConstant.MYSQL_DATABASE_LIST.getValue()))
                .startupOptions(StartupOptions.latest())
                .deserializer(new DebeziumDeserializationSchema())
                .build();

        env.addSource(mysqlSource)
                .addSink(KafkaUtil.getKafkaSink(GetResource.getApplicationPro().getProperty(CommonConstant.ODS_DB_TOPIC.getValue())));

        env.execute();
    }
}
