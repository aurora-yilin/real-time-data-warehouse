package aurora.yilin.realtime.bean;

import org.apache.commons.lang3.ObjectUtils;

import java.math.BigDecimal;

public class OrderWide {
    Long detail_id;
    Long order_id;
    Long sku_id;
    BigDecimal order_price;
    Long sku_num;
    String sku_name;
    Long province_id;
    String order_status;
    Long user_id;

    BigDecimal total_amount;
    BigDecimal activity_reduce_amount;
    BigDecimal coupon_reduce_amount;
    BigDecimal original_total_amount;
    BigDecimal feight_fee;
    BigDecimal split_feight_fee;
    BigDecimal split_activity_amount;
    BigDecimal split_coupon_amount;
    BigDecimal split_total_amount;

    String expire_time;
    String create_time; //yyyy-MM-dd HH:mm:ss
    String operate_time;
    String create_date; // 把其他字段处理得到
    String create_hour;

    String province_name;//查询维表得到
    String province_area_code;
    String province_iso_code;
    String province_3166_2_code;

    Integer user_age;
    String user_gender;

    Long spu_id;     //作为维度数据 要关联进来
    Long tm_id;
    Long category3_id;
    String spu_name;
    String tm_name;
    String category3_name;

    public OrderWide() {
    }

    public OrderWide(Long detail_id, Long order_id, Long sku_id, BigDecimal order_price, Long sku_num, String sku_name, Long province_id, String order_status, Long user_id, BigDecimal total_amount, BigDecimal activity_reduce_amount, BigDecimal coupon_reduce_amount, BigDecimal original_total_amount, BigDecimal feight_fee, BigDecimal split_feight_fee, BigDecimal split_activity_amount, BigDecimal split_coupon_amount, BigDecimal split_total_amount, String expire_time, String create_time, String operate_time, String create_date, String create_hour, String province_name, String province_area_code, String province_iso_code, String province_3166_2_code, Integer user_age, String user_gender, Long spu_id, Long tm_id, Long category3_id, String spu_name, String tm_name, String category3_name) {
        this.detail_id = detail_id;
        this.order_id = order_id;
        this.sku_id = sku_id;
        this.order_price = order_price;
        this.sku_num = sku_num;
        this.sku_name = sku_name;
        this.province_id = province_id;
        this.order_status = order_status;
        this.user_id = user_id;
        this.total_amount = total_amount;
        this.activity_reduce_amount = activity_reduce_amount;
        this.coupon_reduce_amount = coupon_reduce_amount;
        this.original_total_amount = original_total_amount;
        this.feight_fee = feight_fee;
        this.split_feight_fee = split_feight_fee;
        this.split_activity_amount = split_activity_amount;
        this.split_coupon_amount = split_coupon_amount;
        this.split_total_amount = split_total_amount;
        this.expire_time = expire_time;
        this.create_time = create_time;
        this.operate_time = operate_time;
        this.create_date = create_date;
        this.create_hour = create_hour;
        this.province_name = province_name;
        this.province_area_code = province_area_code;
        this.province_iso_code = province_iso_code;
        this.province_3166_2_code = province_3166_2_code;
        this.user_age = user_age;
        this.user_gender = user_gender;
        this.spu_id = spu_id;
        this.tm_id = tm_id;
        this.category3_id = category3_id;
        this.spu_name = spu_name;
        this.tm_name = tm_name;
        this.category3_name = category3_name;
    }

    public OrderWide(OrderInfo orderInfo, OrderDetail orderDetail) {
        mergeOrderInfo(orderInfo);
        mergeOrderDetail(orderDetail);
    }

    public void mergeOrderInfo(OrderInfo orderInfo) {
        if (orderInfo != null) {
            this.order_id = orderInfo.id;
            this.order_status = orderInfo.order_status;
            this.create_time = orderInfo.create_time;
            this.create_date = orderInfo.create_date;
            this.create_hour = orderInfo.create_hour;
            this.activity_reduce_amount = orderInfo.activity_reduce_amount;
            this.coupon_reduce_amount = orderInfo.coupon_reduce_amount;
            this.original_total_amount = orderInfo.original_total_amount;
            this.feight_fee = orderInfo.feight_fee;
            this.total_amount = orderInfo.total_amount;
            this.province_id = orderInfo.province_id;
            this.user_id = orderInfo.user_id;
        }
    }

    public void mergeOrderDetail(OrderDetail orderDetail) {
        if (orderDetail != null) {
            this.detail_id = orderDetail.id;
            this.sku_id = orderDetail.sku_id;
            this.sku_name = orderDetail.sku_name;
            this.order_price = orderDetail.order_price;
            this.sku_num = orderDetail.sku_num;
            this.split_activity_amount = orderDetail.split_activity_amount;
            this.split_coupon_amount = orderDetail.split_coupon_amount;
            this.split_total_amount = orderDetail.split_total_amount;
        }
    }
}
