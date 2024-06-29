package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author name 婉然从物
 * @create 2024-04-23 15:56
 */
public class DwdTradeOrderCancelDetail extends BaseSQLApp {

    public static void main(String[] args) {
        new DwdTradeOrderCancelDetail().start(10024, 4, Constant.TOPIC_DWD_TRADE_ORDER_CANCEL);
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        // TODO 设置TTL， 支付等待时间15s, 支付时间5s
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(15 * 60 + 5));

        // TODO 1、读取topic_db数据
        tableEnv.executeSql("DROP TABLE IF EXISTS topic_db");
        createTopicDb(groupId, tableEnv);

        // TODO 2、从Kafka主题 dwd_trade_order_detail 筛选出订单明细表数据、活动关联表和优惠券关联表
        createTable(tableEnv, groupId);

        // TODO 3、筛选出 upsert 类型 order_status为取消订单 的订单明细表
        filterTable(tableEnv);

        // TODO 4、将  filter_table  表和  order_cancel  表进行join合并
        Table joinTable = getJoinTable(tableEnv);

        // TODO 5、将合并后的表格写出到 dwd_trade_order_cancel_detail Kafka主题
        createResultTable(tableEnv);

        joinTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL).execute();

    }

    private static void createResultTable(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table dwd_trade_order_cancel(\n" +
                "\tid STRING,\n" +
                "\torder_id STRING,\n" +
                "\tsku_id STRING,\n" +
                "\tuser_id STRING,\n" +
                "\tprovince_id STRING,\n" +
                "\tactivity_id STRING,\n" +
                "\tactivity_rule_id STRING,\n" +
                "\tcoupon_id STRING,\n" +
                "\tsku_name STRING,\n" +
                "\torder_price STRING,\n" +
                "\tsku_num STRING,\n" +
                "\tcreate_time STRING,\n" +
                "\tsplit_total_amount STRING,\n" +
                "\tsplit_activity_amount STRING,\n" +
                "\tsplit_coupon_amount STRING,\n" +
                "\tts bigint\n" +
                ")"  + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_ORDER_CANCEL));
    }

    private static Table getJoinTable(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select\n" +
                "\tdtod.id id,\n" +
                "\torder_id,\n" +
                "\tsku_id,\n" +
                "\tuser_id,\n" +
                "\tprovince_id,\n" +
                "\tactivity_id,\n" +
                "\tactivity_rule_id,\n" +
                "\tcoupon_id,\n" +
                "\tsku_name,\n" +
                "\torder_price,\n" +
                "\tsku_num,\n" +
                "\tcreate_time,\n" +
                "\tsplit_total_amount,\n" +
                "\tsplit_activity_amount,\n" +
                "\tsplit_coupon_amount,\n" +
                "\tts\n" +
                "from dwd_trade_order_detail dtod\n" +
                "join order_cancel oc\n" +
                "on dtod.id=oc.id");
    }

    private static void filterTable(StreamTableEnvironment tableEnv) {
        Table orderCancel = tableEnv.sqlQuery("select\n" +
                "\t`data`['id'] id ,\n" +
                "\t`data`['user_id'] user_id ,\n" +
                "\t`data`['province_id'] province_id\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_info'\n" +
                "and `type`='update'\n" +
                "and `data`['order_status']='1003'");
        tableEnv.createTemporaryView("order_cancel", orderCancel);
    }

    private static void createTable(StreamTableEnvironment tableEnv, String groupId) {
        tableEnv.executeSql("create table dwd_trade_order_detail(\n" +
                "\tid STRING,\n" +
                "\torder_id STRING,\n" +
                "\tsku_id STRING,\n" +
                "\tactivity_id STRING,\n" +
                "\tactivity_rule_id STRING,\n" +
                "\tcoupon_id STRING,\n" +
                "\tsku_name STRING,\n" +
                "\torder_price STRING,\n" +
                "\tsku_num STRING,\n" +
                "\tcreate_time STRING,\n" +
                "\tsplit_total_amount STRING,\n" +
                "\tsplit_activity_amount STRING,\n" +
                "\tsplit_coupon_amount STRING,\n" +
                "\tts bigint\n" +
                ") " + SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, groupId));
    }
}