import aurora.yilin.realtime.constant.CommonConstant;
import aurora.yilin.realtime.utils.GetResource;
import aurora.yilin.utils.ThreadPoolUtil;

import java.sql.*;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/7/3
 */
public class PhoenixTest {

    public static void main(String[] args) throws Exception {

        Class.forName(GetResource.getApplicationPro().getProperty(CommonConstant.PHOENIX_DRIVER.getValue()));


        Connection connection = DriverManager.getConnection(GetResource.getApplicationPro().getProperty(CommonConstant.PHOENIX_SERVER.getValue()));
        for (int i = 0; i < 6; i++) {
            int finalI = i;
            ThreadPoolUtil.SINGLE.getThreadPoolExecutor().submit(new Runnable() {
                @Override
                public void run() {
                    PreparedStatement preparedStatement = null;
                    try {
                        preparedStatement = connection.prepareStatement("create table if not exists GMALL210108_REALTIME."+"timeee"+ finalI +"(id varchar primary key ,region_name varchar )");
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                    try {
                        preparedStatement.execute();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }
            });
        }

    }

}
