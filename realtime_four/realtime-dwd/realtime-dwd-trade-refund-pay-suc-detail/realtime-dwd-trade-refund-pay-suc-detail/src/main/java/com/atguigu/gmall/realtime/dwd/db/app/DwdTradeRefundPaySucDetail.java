package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author name 婉然从物
 * @create 2024-04-25 10:26
 */
public class DwdTradeRefundPaySucDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeRefundPaySucDetail().start(10018, 4, "dwd_trade_refund_pay_suc_detail");
    }


    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        // 核心业务
        // TODO 设置TTL为5s
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5L));

        // TODO 1、读取topic_db数据
        tableEnv.executeSql("DROP TABLE IF EXISTS topic_db");
        createTopicDb(groupId, tableEnv);

        // TODO 2、读取字典表数据
        createBaseDic(tableEnv);

        // TODO 3、读取退款支付表，退款成功数据
        Table refundPaymentTable = tableEnv.sqlQuery("select\n" +
                "\t`data`['id'] id,\n" +
                "\t`data`['order_id'] order_id,\n" +
                "\t`data`['sku_id'] sku_id,\n" +
                "\t`data`['payment_type'] payment_type,\n" +
                "\t`data`['callback_time'] callback_time,\n" +
                "\t`data`['total_amount'] total_amount,\n" +
                "\t`ts`\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='refund_payment'\n" +
                "and `type`='update'\n" +
                "and `old`['refund_status'] is not null\n" +
                "and `data`['refund_status']='1602'");
        tableEnv.createTemporaryView("refund_payment", refundPaymentTable);

        // TODO 4、读取退单表数据，过滤出退款成功数据
        Table orderRefundInfoTable = tableEnv.sqlQuery("select\n" +
                "\t`data`['order_id'] order_id,\n" +
                "\t`data`['sku_id'] sku_id,\n" +
                "\t`data`['refund_num'] refund_num\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_refund_info'\n" +
                "and `type`='update'\n" +
                "and `old`['refund_status'] is not null\n" +
                "and `data`['refund_status'] = '0705'");
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfoTable);

        // TODO 5、过滤订单表中退款成功的数据
        Table orderInfoTable = tableEnv.sqlQuery("select\n" +
                "\t`data`['id'] id,\n" +
                "\t`data`['user_id'] user_id,\n" +
                "\t`data`['province_id'] province_id\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_info'\n" +
                "and `type`='update'\n" +
                "and `old`['order_status'] is not null\n" +
                "and `data`['order_status'] = '1006'");
        tableEnv.createTemporaryView("order_info", orderInfoTable);

        // TODO 6、将refund_payment作为主表，关联退单表、字典表和订单表
        Table resultTable = tableEnv.sqlQuery("select\n" +
                "\trp.id,\n" +
                "\tuser_id,\n" +
                "\trp.order_id,\n" +
                "\trp.sku_id,\n" +
                "\tprovince_id,\n" +
                "\tpayment_type,\n" +
                "\tbd.dic_name payment_type_name,\n" +
                "\tcallback_time,\n" +
                "\ttotal_amount,\n" +
                "\trefund_num,\n" +
                "\trp.ts\n" +
                "from refund_payment rp\n" +
                "join order_refund_info ori\n" +
                "on rp.order_id = ori.order_id\n" +
                "and rp.sku_id = ori.sku_id\n" +
                "join order_info oi\n" +
                "on rp.order_id = oi.id\n" +
                "join base_dic bd\n" +
                "on rp.payment_type = bd.rowkey");

        // TODO 7、写出到Kafka对应主题
        tableEnv.executeSql("create table " +  Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS + " (\n" +
                "\tid STRING,\n" +
                "\tuser_id STRING,\n" +
                "\torder_id STRING,\n" +
                "\tsku_id STRING,\n" +
                "\tprovince_id STRING,\n" +
                "\tpayment_type STRING,\n" +
                "\tpayment_type_name STRING,\n" +
                "\tcallback_time STRING,\n" +
                "\ttotal_amount STRING,\n" +
                "\trefund_num STRING,\n" +
                "\tts bigint\n" +
                ")" + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS));

        resultTable.insertInto(Constant.TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS).execute();
    }
}
