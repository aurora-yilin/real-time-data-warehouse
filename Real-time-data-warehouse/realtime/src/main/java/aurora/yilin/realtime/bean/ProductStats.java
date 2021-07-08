package aurora.yilin.realtime.bean;

import aurora.yilin.realtime.utils.annotation.TransientSink;

import java.math.BigDecimal;

/**
 * @Description
 * @Author yilin
 * @Version V1.0.0
 * @Since 1.0
 * @Date 2021/7/7
 */
public class ProductStats {
    String stt;//窗口起始时间
    String edt;  //窗口结束时间
    Long sku_id; //sku编号
    String sku_name;//sku名称
    BigDecimal sku_price; //sku单价
    Long spu_id; //spu编号
    String spu_name;//spu名称
    Long tm_id; //品牌编号
    String tm_name;//品牌名称
    Long category3_id;//品类编号
    String category3_name;//品类名称


    Long display_ct = 0L; //曝光数


    Long click_ct = 0L;  //点击数


    Long favor_ct = 0L; //收藏数


    Long cart_ct = 0L;  //添加购物车数


    Long order_sku_num = 0L; //下单商品个数

    //下单商品金额
    BigDecimal order_amount = BigDecimal.ZERO;


    Long order_ct = 0L; //订单数

    //支付金额
    BigDecimal payment_amount = BigDecimal.ZERO;


    Long paid_order_ct = 0L;  //支付订单数


    Long refund_order_ct = 0L; //退款订单数


    BigDecimal refund_amount = BigDecimal.ZERO;


    Long comment_ct = 0L;//评论订单数


    Long good_comment_ct = 0L; //好评订单数


    @TransientSink
    Set orderIdSet = new HashSet();  //用于统计订单数


    @TransientSink
    Set paidOrderIdSet = new HashSet(); //用于统计支付订单数


    @TransientSink
    Set refundOrderIdSet = new HashSet();//用于退款支付订单数

    Long ts; //统计时间戳
}
