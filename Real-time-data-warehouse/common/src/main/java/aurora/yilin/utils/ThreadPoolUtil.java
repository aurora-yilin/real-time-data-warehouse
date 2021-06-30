package aurora.yilin.utils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/6/30
 */
public enum ThreadPoolUtil {
    SINGLE;
    private ThreadPoolExecutor threadPoolExecutor;

    ThreadPoolUtil(){
        this.threadPoolExecutor = new ThreadPoolExecutor(
                4,
                20,
                300L,
                TimeUnit.SECONDS,new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE));
    }

    public ThreadPoolExecutor getThreadPoolExecutor() {
        return threadPoolExecutor;
    }
}
