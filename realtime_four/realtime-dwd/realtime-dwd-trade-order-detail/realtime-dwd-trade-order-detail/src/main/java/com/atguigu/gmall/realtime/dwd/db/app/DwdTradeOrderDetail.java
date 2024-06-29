package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author name 婉然从物
 * @create 2024-04-23 11:16
 */
public class DwdTradeOrderDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(10024, 4, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        // flinkSQL 在join的时候一定要设置状态存活时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5L));

        // 核心业务编写
        // TODO 1、读取topic_db数据
        tableEnv.executeSql("DROP TABLE IF EXISTS topic_db");
        createTopicDb(groupId, tableEnv);

        // TODO 2、筛选订单详情表数据
        filterOd(tableEnv);

        // TODO 3、筛选订单信息表
        filterOi(tableEnv);

        // TODO 4、筛选订单详情活动关联表
        filterOad(tableEnv);

        // TODO 5、筛选订单详情优惠券关联表
        filterOdc(tableEnv);

        // TODO 6、将四张表格join合并
        Table joinTable = getJoinTable(tableEnv);

        // TODO 7、写出到Kafka中
        // TODO 一旦使用 left join 会产生撤回流 此时如果需要将数据写出到Kafka，必须使用upsert kafka
        createUpsertKafkaSink(tableEnv);

        joinTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL).execute();

    }

    private static void createUpsertKafkaSink(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_ORDER_DETAIL + " (\n" +
                "\tid STRING,\n" +
                "\torder_id STRING,\n" +
                "\tsku_id STRING,\n" +
                "\tuser_id STRING,\n" +
                "\tactivity_id STRING,\n" +
                "\tactivity_rule_id STRING,\n" +
                "\tcoupon_id STRING,\n" +
                "\tprovince_id STRING,\n" +
                "\tsku_name STRING,\n" +
                "\torder_price STRING,\n" +
                "\tsku_num STRING,\n" +
                "\tcreate_time STRING,\n" +
                "\tsplit_total_amount STRING,\n" +
                "\tsplit_activity_amount STRING,\n" +
                "\tsplit_coupon_amount STRING,\n" +
                "\tts bigint,\n" +
                "\tPRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + SQLUtil.getUpsertKafkaSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));
    }

    private static Table getJoinTable(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select\n" +
                "\tod.id,\n" +
                "\torder_id,\n" +
                "\tsku_id,\n" +
                "\tuser_id,\n" +
                "\tactivity_id,\n" +
                "\tactivity_rule_id,\n" +
                "\tcoupon_id,\n" +
                "\tprovince_id,\n" +
                "\tsku_name,\n" +
                "\torder_price,\n" +
                "\tsku_num,\n" +
                "\tcreate_time,\n" +
                "\tsplit_total_amount,\n" +
                "\tsplit_activity_amount,\n" +
                "\tsplit_coupon_amount,\n" +
                "\tts\n" +
                "from order_detail od\n" +
                "join order_info oi\n" +
                "on od.order_id=oi.id\n" +
                "left join order_detail_activity oda\n" +
                "on od.id=oda.id\n" +
                "left join order_detail_coupon odc\n" +
                "on od.id=odc.id");
    }

    private static void filterOdc(StreamTableEnvironment tableEnv) {
        Table odcTable = tableEnv.sqlQuery("select\n" +
                "\t`data`['order_detail_id'] id ,\n" +
                "\t`data`['coupon_id'] coupon_id\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_detail_coupon'\n" +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_detail_coupon", odcTable);
    }

    private static void filterOad(StreamTableEnvironment tableEnv) {
        Table odaTable = tableEnv.sqlQuery("select\n" +
                "\t`data`['order_detail_id'] id ,\n" +
                "\t`data`['activity_id'] activity_id ,\n" +
                "\t`data`['activity_rule_id'] activity_rule_id \n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_detail_activity'\n" +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_detail_activity", odaTable);
    }

    private static void filterOi(StreamTableEnvironment tableEnv) {
        Table oiTable = tableEnv.sqlQuery("select\n" +
                "\t`data`['id'] id ,\n" +
                "\t`data`['user_id'] user_id ,\n" +
                "\t`data`['province_id'] province_id \n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_info'\n" +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_info", oiTable);
    }

    private static void filterOd(StreamTableEnvironment tableEnv) {
        Table odTable = tableEnv.sqlQuery("select\n" +
                "\t`data`['id'] id,\n" +
                "\t`data`['order_id'] order_id,\n" +
                "\t`data`['sku_id'] sku_id,\n" +
                "\t`data`['sku_name'] sku_name,\n" +
                "\t`data`['order_price'] order_price,\n" +
                "\t`data`['sku_num'] sku_num,\n" +
                "\t`data`['create_time'] create_time,\n" +
                "\t`data`['split_total_amount'] split_total_amount,\n" +
                "\t`data`['split_activity_amount'] split_activity_amount,\n" +
                "\t`data`['split_coupon_amount'] split_coupon_amount,\n" +
                "\t`ts`\n" +
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "and `table` = 'order_detail'\n" +
                "and `type` = 'insert' ");
        tableEnv.createTemporaryView("order_detail", odTable);
    }
}
