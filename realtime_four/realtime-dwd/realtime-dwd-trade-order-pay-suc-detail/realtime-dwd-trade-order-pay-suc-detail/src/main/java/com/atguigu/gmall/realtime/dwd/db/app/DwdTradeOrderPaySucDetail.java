package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author name 婉然从物
 * @create 2024-04-24 15:01
 */
public class DwdTradeOrderPaySucDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetail().start(10016, 4, "dwd_trade_order_pay_suc_detail");
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        // 核心业务逻辑
        // TODO 1、读取topic_db数据
        tableEnv.executeSql("DROP TABLE IF EXISTS topic_db");
        createTopicDb(groupId, tableEnv);

        // TODO 2、筛选出支付成功的数据
        filterPaymentTable(tableEnv);

        // TODO 3、读取下单详情表数据
        createDWDOrderDetail(tableEnv, groupId);

        // TODO 4、创建base_dic字典表
        createBaseDic(tableEnv);

        // TODO 5、使用interval join完成支付成功流和订单详情流数据关联
        intervalJoin(tableEnv);

        // TODO 6、使用lookup join 完成维度退化
        Table resultTable = lookupJoin(tableEnv);

        // TODO 7、创建 upsert kafka 写出
        createUpsertKafkaSink(tableEnv);

        resultTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS).execute();
    }

    private static void createUpsertKafkaSink(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS + "  (\n" +
                "\tid STRING,\n" +
                "\torder_id STRING,\n" +
                "\tuser_id STRING,\n" +
                "\tpayment_type_code STRING,\n" +
                "\tpayment_type_name STRING,\n" +
                "\tpayment_time STRING,\n" +
                "\tsku_id STRING,\n" +
                "\tprovince_id STRING,\n" +
                "\tactivity_id STRING,\n" +
                "\tactivity_rule_id STRING,\n" +
                "\tcoupon_id STRING,\n" +
                "\tsku_name STRING,\n" +
                "\torder_price STRING,\n" +
                "\tsku_num STRING,\n" +
                "\tsplit_total_amount STRING,\n" +
                "\tsplit_activity_amount STRING,\n" +
                "\tsplit_coupon_amount STRING,\n" +
                "\tts bigint,\n" +
                "\tPRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + SQLUtil.getUpsertKafkaSQL(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));
    }

    private static Table lookupJoin(StreamTableEnvironment tableEnv) {
        Table resultTable = tableEnv.sqlQuery("SELECT \n" +
                "\tid,\n" +
                "\torder_id,\n" +
                "\tuser_id,\n" +
                "\tpayment_type payment_type_code,\n" +
                "\tinfo.dic_name payment_type_name,\n" +
                "\tpayment_time,\n" +
                "\tsku_id,\n" +
                "\tprovince_id,\n" +
                "\tactivity_id,\n" +
                "\tactivity_rule_id,\n" +
                "\tcoupon_id,\n" +
                "\tsku_name,\n" +
                "\torder_price,\n" +
                "\tsku_num,\n" +
                "\tsplit_total_amount,\n" +
                "\tsplit_activity_amount,\n" +
                "\tsplit_coupon_amount,\n" +
                "\tts\t\n" +
                "from pay_order p\n" +
                "left join base_dic FOR SYSTEM_TIME AS OF p.proc_time AS b\n" +
                "on p.payment_type=b.rowkey");
        return resultTable;
    }

    private static void intervalJoin(StreamTableEnvironment tableEnv) {
        Table payorderTable = tableEnv.sqlQuery("SELECT \n" +
                "\tod.id,\n" +
                "\tp.order_id,\n" +
                "\tp.user_id,\n" +
                "\tpayment_type,\n" +
                "\tcallback_time payment_time,\n" +
                "\tsku_id,\n" +
                "\tprovince_id,\n" +
                "\tactivity_id,\n" +
                "\tactivity_rule_id,\n" +
                "\tcoupon_id,\n" +
                "\tsku_name,\n" +
                "\torder_price,\n" +
                "\tsku_num,\n" +
                "\tsplit_total_amount,\n" +
                "\tsplit_activity_amount,\n" +
                "\tsplit_coupon_amount,\n" +
                "\tp.ts\t,\n" +
                "\tp.proc_time\t\n" +
                "FROM payment p, order_detail od\n" +
                "WHERE p.order_id = od.order_id\n" +
                "AND p.row_time BETWEEN od.row_time - INTERVAL '15' MINUTE AND od.row_time + INTERVAL '5' SECOND");

        tableEnv.createTemporaryView("pay_order",payorderTable);
    }

    private static void createDWDOrderDetail(StreamTableEnvironment tableEnv, String groupId) {
        tableEnv.executeSql("create table order_detail (\n" +
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
                "\t`row_time` as TO_TIMESTAMP_LTZ(ts * 1000, 3),\n" +
                "\tWATERMARK FOR row_time AS row_time - INTERVAL '15' SECOND\n" +
                ")" + SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL, groupId));
    }

    private static void filterPaymentTable(StreamTableEnvironment tableEnv) {
        Table paymentTable = tableEnv.sqlQuery("select\n" +
                "\t`data`['id'] id,\n" +
                "\t`data`['order_id'] order_id,\n" +
                "\t`data`['user_id'] user_id,\n" +
                "\t`data`['payment_type'] payment_type,\n" +
                "\t`data`['total_amount'] total_amount,\n" +
                "\t`data`['callback_time'] callback_time,\n" +
                "\t`ts`,\n" +
                "\t`row_time`,\n" +
                "\t`proc_time`\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='payment_info'\n" +
                "and `type`='update'\n" +
                "and `old`['payment_status'] is not NULL\n" +
                "and `data`['payment_status']='1602'");

        tableEnv.createTemporaryView("payment", paymentTable);
    }
}
