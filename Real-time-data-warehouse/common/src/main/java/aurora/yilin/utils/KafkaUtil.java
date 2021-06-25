package aurora.yilin.utils;

import aurora.yilin.Constant.KafkaConstant;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/6/25
 */
public class KafkaUtil {
    public static <IN> FlinkKafkaProducer<IN> getKafkaSink(String topicId, SerializationSchema<IN> serializationSchema, Properties producerConfig){
        return new FlinkKafkaProducer<IN>(topicId,serializationSchema,producerConfig);
    }

    public static FlinkKafkaProducer<String> getKafkaSink(String topicId,Properties producerConfig){
        return new FlinkKafkaProducer<String>(topicId, new SimpleStringSchema(),producerConfig);
    }

    public static FlinkKafkaProducer getKafkaSink(String topicId){

        Properties properties = new Properties();
        properties.setProperty(KafkaConstant.BOOTSTRAP_SERVERS.getValue(),PropertiesAnalysisUtil.getInfoBykeyFromPro(KafkaConstant.BOOTSTRAP_SERVERS.getValue()));
        return new FlinkKafkaProducer<String>(topicId,new SimpleStringSchema(),properties);
    }
}
