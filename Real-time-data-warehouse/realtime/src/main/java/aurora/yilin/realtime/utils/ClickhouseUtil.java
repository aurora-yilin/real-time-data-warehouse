package aurora.yilin.realtime.utils;

import aurora.yilin.realtime.constant.CommonConstant;
import aurora.yilin.realtime.utils.annotation.TransientSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/7/6
 */
public class ClickhouseUtil {
    private static final Logger log = LoggerFactory.getLogger(ClickhouseUtil.class);

    public static <T> SinkFunction<T> clickHouseJdbcSink(String sql) {
        return JdbcSink.<T>sink(sql,
                new JdbcStatementBuilder<T>() {
                    private int offset = 0;

                    @Override
                    public void accept(PreparedStatement preparedStatement, T object) throws SQLException {
                        Field[] declaredFields = object.getClass().getDeclaredFields();
                        for (int i = 0; i < declaredFields.length; i++) {
                            Field declaredField = declaredFields[i];
                            declaredField.setAccessible(true);

                            try {
                                if (Objects.nonNull(declaredField.getDeclaredAnnotation(TransientSink.class))) {
                                    ++offset;
                                }
                                Object temp = declaredField.get(object);
                                preparedStatement.setObject(i - offset, temp);
                            } catch (NullPointerException | IllegalAccessException np) {
                                log.error(String.valueOf(np.getStackTrace()));
                            }
                        }
                    }
                },
                new JdbcExecutionOptions
                        .Builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GetResource.getApplicationPro().getProperty(CommonConstant.CLICKHOUSE_DRIVER.getValue()))
                        .withUrl(GetResource.getApplicationPro().getProperty(CommonConstant.CLICKHOUSE_URL.getValue()))
                        .build()
        );
    }
}
