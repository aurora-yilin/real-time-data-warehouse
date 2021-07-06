package aurora.yilin.utils;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/6/29
 */
public class JdbcUtil {

    /**
     * 查询sql资源
     * @param sql sql语句
     * @param connection jdbcConnection
     * @param clazz 用于封装查询结果的对象类
     * @param transaction 是否开启下划线转小驼峰的映射
     * @param <T> 封装查询结果对象的泛型
     * @return
     * @throws SQLException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws InvocationTargetException
     */
    public static<T> List<T> query(String sql, Connection connection, Class<T> clazz, Boolean transaction) throws SQLException, IllegalAccessException, InstantiationException, InvocationTargetException {
        ArrayList<T> resultList = new ArrayList<>();

        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        ResultSet resultSet = preparedStatement.executeQuery();

        //获取查询结果的元数据
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        //获取查询结果的列数
        int columnCount = resultSetMetaData.getColumnCount();
        //遍历每一行数据
        while (resultSet.next()) {

            //通过反射根据传入的javaBean对象的类型创建出一个对象用来存储查询结果中的一条数据
            T tempObject = clazz.newInstance();

            for (int i = 1; i <= columnCount; i++) {
                String columnName = resultSetMetaData.getColumnName(i);
                if (transaction){
                    //将数据库中的下划线命名规则转化为小驼峰命名规则
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName);
                }
                BeanUtils.setProperty(tempObject,columnName,resultSet.getString(i));
            }
            resultList.add(tempObject);
        }

        return resultList;
    }
}
