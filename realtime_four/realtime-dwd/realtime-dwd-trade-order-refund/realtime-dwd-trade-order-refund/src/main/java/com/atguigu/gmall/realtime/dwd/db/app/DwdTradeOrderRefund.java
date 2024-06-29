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
 * @create 2024-04-24 19:02
 */
public class DwdTradeOrderRefund extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderRefund().start(10017, 4, Constant.TOPIC_DWD_TRADE_ORDER_REFUND);
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        // TODO 设置TTL时间为5s
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5L));
        // TODO 1、读取topic_db数据
        tableEnv.executeSql("DROP TABLE IF EXISTS topic_db");
        createTopicDb(groupId, tableEnv);

        // TODO 2、读取字典表
        createBaseDic(tableEnv);

        // TODO 3、筛选退单表数据
        getRefundTable(tableEnv);

        // TODO 4、筛选订单表数据
        getOrderInfoTable(tableEnv);

        // TODO 5、关联表,用 order_refund_info 表 关联 order_info和 base_dic表
        Table resultTable = getResultTable(tableEnv);

        // TODO 6、写出到Kafka对应的主题
        createKafkaTable(tableEnv);

        resultTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_REFUND).execute();
    }

    private static void createKafkaTable(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_ORDER_REFUND + " (\n" +
                "\tid STRING,\n" +
                "\tprovince_id STRING,\n" +
                "\tuser_id STRING,\n" +
                "\torder_id STRING,\n" +
                "\tsku_id STRING,\n" +
                "\trefund_type STRING,\n" +
                "\trefund_num STRING,\n" +
                "\trefund_amount STRING,\n" +
                "\trefund_reason_type STRING,\n" +
                "\trefund_reason_txt STRING,\n" +
                "\tcreate_time STRING,\n" +
                "\trowkey STRING,\n" +
                "\tdic_name STRING,\n" +
                "\tts bigint\n" +
                ")" + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_ORDER_REFUND));
    }

    private static Table getResultTable(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select\n" +
                "\toi.id,\n" +
                "\tprovince_id,\n" +
                "\tuser_id,\n" +
                "\torder_id,\n" +
                "\tsku_id,\n" +
                "\trefund_type,\n" +
                "\trefund_num,\n" +
                "\trefund_amount,\n" +
                "\trefund_reason_type,\n" +
                "\trefund_reason_txt,\n" +
                "\tcreate_time,\n" +
                "\trowkey,\n" +
                "\tdic_name,\n" +
                "\tts\n" +
                "from order_info oi\n" +
                "join order_refund_info ori\n" +
                "on ori.order_id=oi.id\n" +
                "join base_dic bd\n" +
                "on ori.refund_type = bd.rowkey");
    }

    private static void getOrderInfoTable(StreamTableEnvironment tableEnv) {
        Table orderInfoTable = tableEnv.sqlQuery("select\n" +
                "\t`data`['id'] id ,\n" +
                "\t`data`['province_id'] province_id\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_info'\n" +
                "and `type`='update'\n" +
                "and `old`['order_status'] is not null\n" +
                "and `data`['order_status']='1005'\n");
        tableEnv.createTemporaryView("order_info", orderInfoTable);
    }

    private static void getRefundTable(StreamTableEnvironment tableEnv) {
        Table refundTable = tableEnv.sqlQuery("select\n" +
                "\t`data`['id'] id, \n" +
                "\t`data`['user_id'] user_id, \n" +
                "\t`data`['order_id'] order_id, \n" +
                "\t`data`['sku_id'] sku_id, \n" +
                "\t`data`['refund_type'] refund_type, \n" +
                "\t`data`['refund_num'] refund_num, \n" +
                "\t`data`['refund_amount'] refund_amount, \n" +
                "\t`data`['refund_reason_type'] refund_reason_type, \n" +
                "\t`data`['refund_reason_txt'] refund_reason_txt, \n" +
                "\t`data`['create_time'] create_time, \n" +
                "\t`proc_time`,\n" +
                "\t`ts`\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_refund_info'\n" +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_refund_info", refundTable);
    }
}