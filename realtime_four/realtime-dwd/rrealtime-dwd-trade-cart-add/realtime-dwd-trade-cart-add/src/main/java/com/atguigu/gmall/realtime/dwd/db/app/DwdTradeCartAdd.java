package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author name 婉然从物
 * @create 2024-04-23 9:06
 */
public class DwdTradeCartAdd extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeCartAdd().start(10013, 4, Constant.TOPIC_DWD_TRADE_CART_ADD);
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        // 核心业务逻辑
        // TODO 1、读取topic_db 的数据
        tableEnv.executeSql("DROP TABLE IF EXISTS topic_db");
        createTopicDb(groupId, tableEnv);

        // TODO 2、筛选加购数据
        Table cartAddTable = filterCartAdd(tableEnv);

        // TODO 3、创建KafkaSink输出映射
        createKafkaSinkTable(tableEnv);

        // TODO 4、写出筛选的数据到对应的Kafka主题
        cartAddTable.insertInto(Constant.TOPIC_DWD_TRADE_CART_ADD).execute();
    }

    private static void createKafkaSinkTable(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_CART_ADD + "(\n" +
                "\tid STRING,\n" +
                "\tuser_id STRING,\n" +
                "\tsku_id STRING,\n" +
                "\tcart_price STRING,\n" +
                "\tsku_num STRING,\n" +
                "\tsku_name STRING,\n" +
                "\tis_checked STRING,\n" +
                "\tcreate_time STRING,\n" +
                "\toperate_time STRING,\n" +
                "\tis_ordered STRING,\n" +
                "\torder_time STRING,\n" +
                "\tts bigint)"
        + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_CART_ADD));
    }

    private static Table filterCartAdd(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select\n" +
                "`data`['id'] id,\n" +
                "`data`['user_id'] user_id,\n" +
                "`data`['sku_id'] sku_id,\n" +
                "`data`['cart_price'] cart_price,\n" +
                "if(`type`='insert',`data`['sku_num'], cast(cast(`data`['sku_num'] as bigint) - cast(`old`['sku_num'] as bigint) as string)),\n" +
                "`data`['sku_name'] sku_name,\n" +
                "`data`['is_checked'] is_checked,\n" +
                "`data`['create_time'] create_time,\n" +
                "`data`['operate_time'] operate_time,\n" +
                "`data`['is_ordered'] is_ordered,\n" +
                "`data`['order_time'] order_time,\n" +
                "`ts`\n" +
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "and `table` = 'cart_info'\n" +
                "and (`type` = 'insert' OR (\n" +
                "\t`type` = 'update' and `old`['sku_num'] is not NULL\n" +
                "\tand cast(`data`['sku_num'] as bigint) > cast(`old`['sku_num'] as bigint)))");
    }
}
