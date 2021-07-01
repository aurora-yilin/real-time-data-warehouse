package aurora.yilin.realtime.utils.func;

import com.alibaba.fastjson.JSONObject;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/7/1
 */
public interface DimAsyncI<T> {
    String getKey(T input);

    void join(T input, JSONObject dimInfo);
}
