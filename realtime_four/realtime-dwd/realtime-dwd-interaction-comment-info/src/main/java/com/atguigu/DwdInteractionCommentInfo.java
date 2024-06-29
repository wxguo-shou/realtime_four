package com.atguigu;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author name 婉然从物
 * @create 2024-04-22 11:20
 */
public class DwdInteractionCommentInfo extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(10012, 4, "dwd_interaction_comment_info");
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        // 核心业务逻辑
        // 创建之前先 删除 topic_db
        tableEnv.executeSql("DROP TABLE IF EXISTS topic_db");

        // TODO 1. 读取topic_db
        createTopicDb(groupId, tableEnv);

        // TODO 2. 读取base_dic
        createBaseDic(tableEnv);

        // TODO 3. 清洗topic_db   筛选出评论信息表新增的数据
        filterCommentInfo(tableEnv);

        // TODO 4. 使用lookUp join 完成维度退化
        Table joinTable = lookUpJoin(tableEnv);

        // TODO 5. 创建kafka sink 对应的表格
        createKafkaSinkTable(tableEnv);

        // TODO 6. 写出到对应的 Kafka 主题
        joinTable.insertInto(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO).execute();

    }

    private static void createKafkaSinkTable(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO + "(" +
                "id STRING,\n" +
                "user_id STRING,\n" +
                "nick_name STRING,\n" +
                "sku_id STRING,\n" +
                "spu_id STRING,\n" +
                "order_id STRING,\n" +
                "appraise_code STRING,\n" +
                "appraise_name STRING,\n" +
                "comment_txt STRING,\n" +
                "create_time STRING,\n" +
                "operate_time STRING" + ")"
                + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO)
        );
    }

    private static Table lookUpJoin(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select\n" +
                "\tid,\n" +
                "\tuser_id,\n" +
                "\tnick_name,\n" +
                "\tsku_id,\n" +
                "\tspu_id,\n" +
                "\torder_id,\n" +
                "\tappraise appraise_code,\n" +
                "\tinfo.dic_name appraise_name,\n" +
                "\tcomment_txt,\n" +
                "\tc.create_time,\n" +
                "\tc.operate_time\n" +
                "from comment_info c\n" +
                "join base_dic FOR SYSTEM_TIME AS OF c.proc_time AS b\n" +
                "on c.appraise = b.rowkey\n" +
                ";");
    }

    private static void filterCommentInfo(StreamTableEnvironment tableEnv) {
        Table commentInfo = tableEnv.sqlQuery("select\n" +
                "\t`data`['id'] id,\n" +
                "\t`data`['user_id'] user_id,\n" +
                "\t`data`['nick_name'] nick_name,\n" +
                "\t`data`['head_img'] head_img,\n" +
                "\t`data`['sku_id'] sku_id,\n" +
                "\t`data`['spu_id'] spu_id,\n" +
                "\t`data`['order_id'] order_id,\n" +
                "\t`data`['appraise'] appraise,\n" +
                "\t`data`['comment_txt'] comment_txt,\n" +
                "\t`data`['create_time'] create_time,\n" +
                "\t`data`['operate_time'] operate_time,\n" +
                "\t`proc_time`\n" +
                "from topic_db\n" +
                "where `database` = 'gmall'\n" +
                "and `table` = 'comment_info'\n" +
                "and `type` = 'insert'\n" +
                ";");

        tableEnv.createTemporaryView("comment_info", commentInfo);
    }
}