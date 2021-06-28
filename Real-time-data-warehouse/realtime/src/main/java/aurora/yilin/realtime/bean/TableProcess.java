package aurora.yilin.realtime.bean;

import aurora.yilin.realtime.constant.CommonConstant;
import aurora.yilin.realtime.utils.GetResource;

import java.io.Serializable;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/6/28
 */
public class TableProcess implements Serializable {

    //动态分流Sink常量
    public transient static final String SINK_TYPE_HBASE = GetResource.getApplicationPro().getProperty(CommonConstant.SINK_TYPE_HBASE.getValue());
    public transient static final String SINK_TYPE_KAFKA = GetResource.getApplicationPro().getProperty(CommonConstant.SINK_TYPE_KAFKA.getValue());
    public transient static final String SINK_TYPE_CK = GetResource.getApplicationPro().getProperty(CommonConstant.SINK_TYPE_CK.getValue());
    //来源表
    String sourceTable;
    //操作类型 insert,update,delete
    String operateType;
    //输出类型 hbase kafka
    String sinkType;
    //输出表(主题)
    String sinkTable;
    //输出字段
    String sinkColumns;
    //主键字段
    String sinkPk;
    //建表扩展
    String sinkExtend;

    public TableProcess(String sourceTable, String operateType, String sinkType, String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        this.sourceTable = sourceTable;
        this.operateType = operateType;
        this.sinkType = sinkType;
        this.sinkTable = sinkTable;
        this.sinkColumns = sinkColumns;
        this.sinkPk = sinkPk;
        this.sinkExtend = sinkExtend;
    }

    public TableProcess() {
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public String getOperateType() {
        return operateType;
    }

    public void setOperateType(String operateType) {
        this.operateType = operateType;
    }

    public String getSinkType() {
        return sinkType;
    }

    public void setSinkType(String sinkType) {
        this.sinkType = sinkType;
    }

    public String getSinkTable() {
        return sinkTable;
    }

    public void setSinkTable(String sinkTable) {
        this.sinkTable = sinkTable;
    }

    public String getSinkColumns() {
        return sinkColumns;
    }

    public void setSinkColumns(String sinkColumns) {
        this.sinkColumns = sinkColumns;
    }

    public String getSinkPk() {
        return sinkPk;
    }

    public void setSinkPk(String sinkPk) {
        this.sinkPk = sinkPk;
    }

    public String getSinkExtend() {
        return sinkExtend;
    }

    public void setSinkExtend(String sinkExtend) {
        this.sinkExtend = sinkExtend;
    }
}
