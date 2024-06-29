package com.atguigu.gmall.realtime.dws.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import com.atguigu.gmall.realtime.dws.function.KwSplit;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author name 婉然从物
 * @create 2024-04-27 11:25
 */
public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLApp {
    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(10021, 4, "dws_traffic_source_keyword_page_view_window");
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        // 核心业务逻辑
        // TODO 1、读取主流DWD页面主题数据
        createPageInfo(tableEnv, groupId);

        // TODO 2、筛选出关键字keywords
        filterKeywords(tableEnv);

        // TODO 3、自定义UDTF分词函数   并注册
        tableEnv.createTemporarySystemFunction("KwSplit", KwSplit.class);

        // TODO 4、调用分词函数对keywords进行拆分
        KwSplit(tableEnv);

        // TODO 5、对keyword进行分组开窗聚合
        Table windowAggTable = getWindowAggTable(tableEnv);

        // TODO 6、写出到doris
        // flink需要开启检查点，才能将数据写出到doris
        createDorisSink(tableEnv);

        windowAggTable.insertInto("doris_sink").execute();
    }

    private static void createDorisSink(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("CREATE TABLE doris_sink (\n" +
                "    stt STRING,\n" +
                "    edt STRING,\n" +
                "    cur_date STRING,\n" +
                "    keyword STRING,\n" +
                "    keyword_count bigint\n" +
                ") \n" + SQLUtil.getDorisSinkSQL(Constant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW) );
    }

    private static Table getWindowAggTable(StreamTableEnvironment tableEnv) {
        Table windowAggTable = tableEnv.sqlQuery("SELECT\n" +
                "  cast(TUMBLE_START(row_time, INTERVAL '10' SECOND) as STRING) AS stt,\n" +
                "  cast(TUMBLE_END(row_time, INTERVAL '10' SECOND) as STRING) AS edt,\n" +
                "  cast(CURRENT_DATE as STRING) cur_date,\n" +
                "  keyword,\n" +
                "  count(*) keyword_count\n" +
                "FROM keyword_table\n" +
                "GROUP BY\n" +
                "  TUMBLE(row_time, INTERVAL '10' SECOND),\n" +
                "  keyword");
        return windowAggTable;
    }

    private static void KwSplit(StreamTableEnvironment tableEnv) {
        Table keywordTable = tableEnv.sqlQuery(
                "SELECT keywords, keyword, `row_time` " +
                        "FROM keywords_table " +
                        "LEFT JOIN LATERAL TABLE(KwSplit(keywords)) ON TRUE");
        tableEnv.createTemporaryView("keyword_table", keywordTable);
    }

    private static void filterKeywords(StreamTableEnvironment tableEnv) {
        Table keywordsTable = tableEnv.sqlQuery("select\n" +
                "\tpage['item'] keywords,\n" +
                "\t`row_time`\n" +
                "from page_info\n" +
                "where page['last_page_id'] = 'search'\n" +
                "and page['item_type'] = 'keyword'\n" +
                "and page['item'] is not null");
        tableEnv.createTemporaryView("keywords_table", keywordsTable);
    }

    private static void createPageInfo(StreamTableEnvironment tableEnv, String groupId) {
        tableEnv.executeSql("create table page_info(\n" +
                "\t`common` map<STRING,STRING>,\n" +
                "\t`page` map<STRING,STRING>,\n" +
                "\t`ts` bigint,\n" +
                "\t`row_time` as TO_TIMESTAMP_LTZ(ts,3),\n" +
                "\tWATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n" +
                ")" + SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRAFFIC_PAGE, groupId));
    }
}
