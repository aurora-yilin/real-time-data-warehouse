import aurora.yilin.utils.ThreadPoolUtil;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/7/3
 */
public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        KeyedStream<String, String> stringDataStreamSource = env.fromElements("1,", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "121", "1232")
                .keyBy(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String value) throws Exception {
                        return value;
                    }
                });
        KeyedStream<String, String> stringDataStreamSource1 = env.fromElements("5", "6", "7", "8", "9", "10", "11", "121", "1232")
                .keyBy(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String value) throws Exception {
                        return value;
                    }
                });
        ;

        ValueStateDescriptor vs = new ValueStateDescriptor("tstss", String.class);
        stringDataStreamSource.connect(stringDataStreamSource1).process(new CoProcessFunction<String, String, String>() {
            @Override
            public void processElement1(String value, Context ctx, Collector<String> out) throws Exception {
                getRuntimeContext().getState(vs).update("hello,world");
            }

            @Override
            public void processElement2(String value, Context ctx, Collector<String> out) throws Exception {
                ThreadPoolExecutor threadPoolExecutor = ThreadPoolUtil.SINGLE.getThreadPoolExecutor();
                CompletableFuture.supplyAsync(new Supplier<String>() {
                    @Override
                    public String get() {
                        return value;
                    }
                }, threadPoolExecutor)
                .thenAccept(new Consumer<String>() {
                    @Override
                    public void accept(String s) {
                        System.out.println(s + Thread.currentThread().getName());
                    }
                });
                System.out.println(getRuntimeContext().getState(vs).value());
            }
        });

        env.execute();


    }
}
