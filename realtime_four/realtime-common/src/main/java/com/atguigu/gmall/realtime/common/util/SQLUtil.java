package com.atguigu.gmall.realtime.common.util;

import com.atguigu.gmall.realtime.common.constant.Constant;

/**
 * @author name 婉然从物
 * @create 2024-04-22 14:56
 */
public class SQLUtil {
    public static String getKafkaSourceSQL(String topicName, String groupId) {
        return " WITH (\n" +
                "\t'connector' = 'kafka',\n" +
                "\t'topic' = ' " + topicName + " ',\n" +
                "\t'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "\t'properties.group.id' = ' " + groupId + " ',\n" +
                "\t'scan.startup.mode' = 'earliest-offset',\n" +
                "\t'format' = 'json'\n" +
                ");\n";
    }

    public static String getKafkaTopicDb(String groupId){

        return "CREATE TABLE topic_db (\n" +
                "\t`database` STRING,\n" +
                "\t`table` STRING,\n" +
                "\t`data` map<STRING, STRING>,\n" +
                "\t`old` map<STRING, STRING>,\n" +
                "\t`type` STRING,\n" +
                "\t`ts` bigint,\n" +
                "\t`proc_time` as PROCTIME(),\n" +
                "\t`row_time` as TO_TIMESTAMP_LTZ(ts * 1000, 3),\n" +
                "\tWATERMARK FOR row_time AS row_time - INTERVAL '15' SECOND\n" +
                ")" + getKafkaSourceSQL(Constant.TOPIC_DB, groupId);
    }

    public static String getKafkaSinkSQL(String topicName) {
        return " WITH (\n" +
                "\t'connector' = 'kafka',\n" +
                "\t'topic' = ' " + topicName + " ',\n" +
                "\t'properties.bootstrap.servers' = ' " + Constant.KAFKA_BROKERS + " ',\n" +
                "\t'format' = 'json'\n" +
                ");\n";
    }

    /**
     * 获取upser kafka的连接     创建表格的语句最后一定要声明主键
     * @param topicName
     * @return
     */
    public static String getUpsertKafkaSQL(String topicName) {
        return "WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = ' "+ topicName + " ',\n" +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")";
    }

    public static String getDorisSinkSQL(String tableName) {
        return "WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = '" + Constant.FENODES + "',\n" +
                "      'table.identifier' = '" + Constant.DORIS_DATABASE + "." + tableName + "',\n" +
                "      'username' = '" + Constant.DORIS_USERNAME + "',\n" +
                "      'password' = '" + Constant.DORIS_PASSWORD + "',\n" +
                "      'sink.label-prefix' = 'doris_label" + System.currentTimeMillis() + "'\n" +
                ")";
    }

}