package aurora.yilin.constant;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/6/25
 */
public enum MySqlConstant {
    //mysql.hostname=hadoop122
    MYSQL_HOSTNAME("mysql.hostname"),
    //mysql.port=3306
    MYSQL_PORT("mysql.port"),
    //mysql.username=root
    MYSQL_USERNAME("mysql.username"),
    //mysql.password=Natural;follow
    MYSQL_PASSWORD("mysql.password");



    private String value;


    MySqlConstant(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
