package aurora.yilin.dbserver.constant;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/6/26
 */
public enum CommonConstant {
    //ods.db.topic=ods_base_db
    ODS_DB_TOPIC("ods.db.topic"),
    //mysql.databseList=gmall2021
    MYSQL_DATABASE_LIST("mysql.databse.list");

    private String value;


    CommonConstant(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
