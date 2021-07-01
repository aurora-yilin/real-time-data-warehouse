package aurora.yilin.realtime.utils.func;

import aurora.yilin.realtime.bean.OrderWide;
import aurora.yilin.realtime.constant.CommonConstant;
import aurora.yilin.realtime.utils.DimUtil;
import aurora.yilin.realtime.utils.GetResource;
import aurora.yilin.utils.ThreadPoolUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllResourceTypeInfoRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/7/1
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimAsyncI<T>{
    private static final Logger log = LoggerFactory.getLogger(DimAsyncFunction.class);


    private Connection connection;
    private String tableName;

    private ThreadPoolExecutor threadPoolExecutor;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GetResource.getApplicationPro()
                        .getProperty(CommonConstant.PHOENIX_DRIVER.getValue()));
        connection = DriverManager.getConnection(GetResource.getApplicationPro()
                .getProperty(CommonConstant.PHOENIX_SERVER.getValue()));

        threadPoolExecutor = ThreadPoolUtil.SINGLE.getThreadPoolExecutor();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {

        CompletableFuture
                .supplyAsync(new Supplier<T>() {
                    @Override
                    public T get() {
                        String key = getKey(input);
                        JSONObject dimInfoBykey = DimUtil.getDimInfo(connection, tableName, key);
                        if (Objects.nonNull(dimInfoBykey)) {
                            join(input,dimInfoBykey);
                        }
                        return input;
                    }
                }, threadPoolExecutor)
                .thenAccept(new Consumer<T>() {
                    @Override
                    public void accept(T t) {
                        resultFuture.complete(Collections.singleton(t));
                    }
                });
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        log.warn("hbase connection timeout");
    }
}
