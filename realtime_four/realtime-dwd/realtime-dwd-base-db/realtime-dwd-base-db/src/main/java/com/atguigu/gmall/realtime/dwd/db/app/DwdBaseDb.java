package com.atguigu.gmall.realtime.dwd.db.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TableProcessDwd;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import com.atguigu.gmall.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author name 婉然从物
 * @create 2024-04-25 15:38
 */
public class DwdBaseDb extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseDb().start(10018, 4, "dwd_base_db", Constant.TOPIC_DB);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 核心业务逻辑
        // TODO 1、获取topic_db数据 （已完成）
//        stream.print();

        // TODO 2、清洗过滤和转换
        SingleOutputStreamOperator<JSONObject> jsonObjStream = flatMapToJsonObj(stream);

        // TODO 3、读取配置表数据  使用flinkCDC读取
        DataStreamSource<String> tableProcessDwd = env.fromSource(FlinkSourceUtil.getMySqlSource(Constant.PROCESS_DATABASE,
                        Constant.PROCESS_DWD_TABLE_NAME), WatermarkStrategy.noWatermarks(), "table_process_dwd").setParallelism(1);

        // TODO 4、转换数据格式
        SingleOutputStreamOperator<TableProcessDwd> processDwdStream = flatMapToProcessDwd(tableProcessDwd);

        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor = new MapStateDescriptor<>("process_state", String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastStream = processDwdStream.broadcast(mapStateDescriptor);

        // TODO 5、连接主流和广播流， 判断主流数据是否需要保留
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processStream = processBaseDb(jsonObjStream, mapStateDescriptor, broadcastStream);

        // TODO 6、筛选最后需要写出的字段
        SingleOutputStreamOperator<JSONObject> dataStream = filterColumns(processStream);

        // TODO 7、写出到对应的kafka主题
        dataStream.sinkTo(FlinkSinkUtil.getKafkaSinkWithTopicName());
    }

    private static SingleOutputStreamOperator<JSONObject> filterColumns(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processStream) {
        SingleOutputStreamOperator<JSONObject> dataStream = processStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDwd>, JSONObject>() {
            @Override
            public JSONObject map(Tuple2<JSONObject, TableProcessDwd> value) throws Exception {
                JSONObject jsonObj = value.f0;
                TableProcessDwd processDwd = value.f1;
                JSONObject data = jsonObj.getJSONObject("data");
                List<String> columns = Arrays.asList(processDwd.getSinkColumns().split(","));
                data.keySet().removeIf(key -> !columns.contains(key));
                data.put("sink_table", processDwd.getSinkTable());
                return data;
            }
        });
        return dataStream;
    }

    private static SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processBaseDb(SingleOutputStreamOperator<JSONObject> jsonObjStream, MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor, BroadcastStream<TableProcessDwd> broadcastStream) {
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processStream = jsonObjStream.connect(broadcastStream)
                .process(new BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>() {
                    Map<String, TableProcessDwd> hashMap = new HashMap<>();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        Connection mysqlConnection = JdbcUtil.getMysqlConnection();
                        List<TableProcessDwd> tableProcessDwds = JdbcUtil.queryList(mysqlConnection,
                                "select * from gmall2023_config.table_process_dwd", TableProcessDwd.class, true);
                        for (TableProcessDwd tableProcessDwd : tableProcessDwds) {
                            hashMap.put(tableProcessDwd.getSourceTable() + ":" + tableProcessDwd.getSourceType(), tableProcessDwd);
                        }
                    }

                    // 将配置表数据放到广播状态中
                    @Override
                    public void processBroadcastElement(TableProcessDwd value, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
                        BroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        String op = value.getOp();
                        String key = value.getSourceTable() + ":" + value.getSourceType();
                        if ("d".equals(op)) {
                            hashMap.remove(key);
                            broadcastState.remove(key);
                        } else {
                            broadcastState.put(key, value);
                        }

                    }

                    @Override
                    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
                        // 调用广播状态， 判断当前数据是否需要保留
                        String table = value.getString("table");
                        String type = value.getString("type");
                        String key = table + ":" + type;
                        ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        TableProcessDwd processDwd = broadcastState.get(key);
                        // 二次判断 是否是先到的数据
                        if (processDwd == null) {
                            processDwd = hashMap.get(key);
                        }
                        if (processDwd != null) {
                            out.collect(Tuple2.of(value, processDwd));
                        }
                    }

                }).setParallelism(1);
        return processStream;
    }

    private static SingleOutputStreamOperator<TableProcessDwd> flatMapToProcessDwd(DataStreamSource<String> tableProcessDwd) {
        SingleOutputStreamOperator<TableProcessDwd> processDwdStream = tableProcessDwd.flatMap(new FlatMapFunction<String, TableProcessDwd>() {
            @Override
            public void flatMap(String value, Collector<TableProcessDwd> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String op = jsonObject.getString("op");
                    TableProcessDwd processDwd;
                    if ("d".equals(op)) {
                        processDwd = jsonObject.getObject("before", TableProcessDwd.class);
                    } else {
                        processDwd = jsonObject.getObject("after", TableProcessDwd.class);
                    }
                    processDwd.setOp(op);
                    out.collect(processDwd);
                } catch (Exception e) {
                    System.out.println("捕获脏数据" + value);
                }

            }
        }).setParallelism(1);
        return processDwdStream;
    }

    private static SingleOutputStreamOperator<JSONObject> flatMapToJsonObj(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("清洗掉脏数据" + value);
                }
            }
        });
    }
}
