package aurora.yilin.realtime.app.dwd.func;

import aurora.yilin.realtime.bean.TableProcess;
import aurora.yilin.realtime.constant.CommonConstant;
import aurora.yilin.realtime.utils.GetResource;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/7/3
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private static final Logger log = LoggerFactory.getLogger(TableProcessFunction.class);


    private OutputTag<JSONObject> outputTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private Connection connection;

    public TableProcessFunction(OutputTag<JSONObject> outputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.outputTag = outputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化Phoenix链接
        Class.forName(GetResource.getApplicationPro().getProperty(CommonConstant.PHOENIX_DRIVER.getValue()));
        connection = DriverManager.getConnection(GetResource.getApplicationPro().getProperty(CommonConstant.PHOENIX_SERVER.getValue()));
    }

    //value:{"database":"","tableName":"","type":"","data":{"source_table":"base_trademark",...},"before":{"id":"1001",...}}
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        log.info(value + ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

        //提取广播流中的数据信息将其转化为TableProcess对象
        JSONObject jsonObject = JSONObject.parseObject(value);
        log.info(value + "---------------------------------");
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("data"), TableProcess.class);

        //根据解析后的tableProcess对象中的信息通过phoenix创建表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend());
        }

        //将广播流中的信息广播出去
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "_" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);
    }


    //检测phoenix中是否存在表若不存在则创建  create table if not exists yy.xx(aa varchar primary key,bb varchar)

    /**
     * 尝试根据配置文件信息创建表
     *
     * @param sinkTable
     * @param sinkColumns
     * @param sinkPk
     * @param sinkExtend
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        PreparedStatement preparedStatement = null;

        try {
            //校验主键和建表扩展字段，并尝试初始化
            if (sinkPk == null) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }

            //根据提供的建表信息尝试拼接建表语句
            StringBuilder createSql = new StringBuilder("create table if not exists ");
            createSql.append(GetResource.getApplicationPro().getProperty(CommonConstant.HBASE_SCHEMA.getValue()))
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {

                String column = columns[i];
                //判断当前的列是否为主键
                if (sinkPk.equals(column)) {
                    createSql.append(column).append(" varchar primary key ");
                } else {
                    createSql.append(column).append(" varchar ");
                }

                //如果当前字段不是最后一个字段,则需要添加","
                if (i < columns.length - 1) {
                    createSql.append(",");
                }
            }

            //拼接扩展字段
            createSql.append(")").append(sinkExtend);

            //ceshi
            System.out.println(createSql);

            //预编译sql语句
            preparedStatement = connection.prepareStatement(createSql.toString());

            //执行建表语句
            preparedStatement.execute();

        } catch (SQLException e) {
            log.error("Phoenix创建" + sinkTable + "表失败！");
            e.printStackTrace();
            throw new RuntimeException("Phoenix创建" + sinkTable + "表失败！");
        } finally {

            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //value:{"database":"","tableName":"","type":"","data":{"id":"1",...},"before":{"id":"1001",...}}
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

        //获取广播流存储的广播信息
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        //根据当前数据流中数据的信息字段获取对应的广播信息
        String key = value.getString("tableName") + "_" + value.getString("type");
        TableProcess tableProcess = broadcastState.get(key);

        //该数据对应的广播信息不为null
        if (tableProcess != null) {

            //过滤掉jsonObject中不要求保留的字段的信息
            filterColumn(value.getJSONObject("data"), tableProcess.getSinkColumns());

            //给校验过的jsonObject对象添加sinkTable信息
            value.put("sinkTable", tableProcess.getSinkTable());

            //根据JsonObject对象对应的配置信息来选择对应的输出流
            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                //hbase维度数据写入测输出流
                ctx.output(outputTag, value);
            } else if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                //事实数据写入主流
                out.collect(value);
            }

        } else {
            System.out.println("该组合Key：" + key + "不存在配置信息！");
        }

    }

    //根据配置信息过滤字段 data：{"id":"1","tm_name":"atguigu","logo":"xxx"}

    /**
     * 根据配置广播流中的字段信息对主流中数据进行字段过滤
     *
     * @param data
     * @param sinkColumns
     */
    private void filterColumn(JSONObject data, String sinkColumns) {

        //拆分需要保留的字段信息并转化为list结构便于后续操作
        String[] columnsArr = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columnsArr);

        //将JsonObject中所有的字段信息转化成set结构
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        //根据保留字段移除多余的字段
        entries.removeIf(next -> !columnList.contains(next.getKey()));

            /*Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> next = iterator.next();
                if (!columnList.contains(next.getKey())) {
                    iterator.remove();
                }
            }*/

    }
}
