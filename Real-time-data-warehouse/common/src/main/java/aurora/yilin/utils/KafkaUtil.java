package aurora.yilin.utils;

import aurora.yilin.constant.KafkaConstant;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @Description kafka工具包
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


    public static FlinkKafkaConsumer getKafkaSource(String topicId, String groupId){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,PropertiesAnalysisUtil.getInfoBykeyFromPro(KafkaConstant.BOOTSTRAP_SERVERS.getValue()));
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        return new FlinkKafkaConsumer(topicId,new SimpleStringSchema(),properties);
    }

    public static <IN> FlinkKafkaConsumer<IN> getKafkaSource(String topicId, SerializationSchema<IN> serializationSchema, Properties producerConfig){
        return new FlinkKafkaConsumer<IN>(topicId, (DeserializationSchema<IN>) serializationSchema,producerConfig);
    }

    public static FlinkKafkaConsumer<String> getKafkaSource(String topicId, Properties producerConfig){
        return new FlinkKafkaConsumer<String>(topicId,new SimpleStringSchema(),producerConfig);
    }
}
