package com.atguigu.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author name 婉然从物
 * @create 2024-04-20 17:08
 */
public class DwdBaseLog extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseLog().start(10011, 4, "dwd_base_log", Constant.TOPIC_LOG);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 核心业务逻辑
//        stream.print();

        // TODO 1. 进行ETL过滤不完整的数据
        SingleOutputStreamOperator<JSONObject> jsonObjStream = elt(stream);

        // TODO 2. 进行新旧访客修复
        KeyedStream<JSONObject, String> keyedStream = keyByWithWatermark(jsonObjStream);

        SingleOutputStreamOperator<JSONObject> isNewFixStream = isNewFix(keyedStream);
//        isNewFixStream.print();

        // TODO 3. 拆分不同类型的用户行为日志
        // 启动日志： 启动信息   报错信息
        // 页面日志： 页面信息   曝光信息    动作信息    报错信息
//        OutputTag<String> startTag = new OutputTag<String>("start"){};
        OutputTag<String> startTag = new OutputTag<>("start", TypeInformation.of(String.class));
        OutputTag<String> errTag = new OutputTag<>("err", TypeInformation.of(String.class));
        OutputTag<String> displayTag = new OutputTag<>("display", TypeInformation.of(String.class));
        OutputTag<String> actionTag = new OutputTag<>("action", TypeInformation.of(String.class));

        SingleOutputStreamOperator<String> pageStream = splitLog(isNewFixStream, startTag, errTag, displayTag, actionTag);

        SideOutputDataStream<String> startStream = pageStream.getSideOutput(startTag);
        SideOutputDataStream<String> errStream = pageStream.getSideOutput(errTag);
        SideOutputDataStream<String> displayStream = pageStream.getSideOutput(displayTag);
        SideOutputDataStream<String> actionStream = pageStream.getSideOutput(actionTag);

//        pageStream.print("page");
//        startStream.print("start");
//        displayStream.print("display");
//        actionStream.print("action");
//        errStream.printToErr("err");

        pageStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        startStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        displayStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        actionStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
        errStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));

    }

    private static SingleOutputStreamOperator<String> splitLog(SingleOutputStreamOperator<JSONObject> isNewFixStream, OutputTag<String> startTag, OutputTag<String> errTag, OutputTag<String> displayTag, OutputTag<String> actionTag) {
        return isNewFixStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                // 核心逻辑 根据数据的不同 拆分到不同的侧输出流
                JSONObject err = value.getJSONObject("err");
                if (err != null) {
                    // 当前存在报错信息
                    ctx.output(errTag, err.toString());
                }

                JSONObject page = value.getJSONObject("page");
                JSONObject start = value.getJSONObject("start");
                JSONObject common = value.getJSONObject("common");
                Long ts = value.getLong("ts");

                if (start != null) {
                    // 当前是启动日志
                    // 注意   输出的是value完整的日志信息
                    ctx.output(startTag, value.toString());
                } else if (page != null) {
                    // 当前为页面日志
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("ts", ts);
                            display.put("page", page);
                            ctx.output(displayTag, display.toJSONString());
                        }
                        value.remove("displays");
                    }

                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            action.put("ts", ts);
                            action.put("page", page);
                            ctx.output(actionTag, action.toJSONString());
                        }
                        value.remove("actions");
                    }

                    // 只保留page信息 写出到主流
                    out.collect(value.toJSONString());
                } else {
                    // 留空
                }
            }
        });
    }

    public SingleOutputStreamOperator<JSONObject> isNewFix (KeyedStream<JSONObject, String> keyedStream) {
        return keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            ValueState<String> firstLoginDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 创建状态
                firstLoginDtState = getRuntimeContext().getState(new ValueStateDescriptor<>("first_login_ts", String.class));
            }

            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                // 1. 获取当前数据的is_new字段
                JSONObject common = value.getJSONObject("common");
                String isNew = common.getString("is_new");
                String firstLogDt = firstLoginDtState.value();
                Long ts = value.getLong("ts");
                String currentDate = DateFormatUtil.tsToDate(ts);
                if ("1".equals(isNew)) {
                    // 判断当前状态情况
                    if (firstLogDt != null && !firstLogDt.equals(currentDate)) {
                        // 如果状态不为空  日期也不是今天， 说明当前数据错误 不是新访客  伪装新访客
                        common.put("is_new", "0");
                    } else if (firstLogDt == null) {
                        // 状态为空
                        firstLoginDtState.update(currentDate);
                    } else {
                        // 留空
                        // 当天数据是同一天新访客重复登录
                    }
                } else if ("0".equals(isNew)) {
                    // is_new 为 0
                    if (firstLogDt == null) {
                        // 老用户 flink实时数仓里面还没有记录过这个访客  需要补充访客信息
                        // 把访客首次登录日期补充一个值   今天以前的任意一天都可以    这儿使用昨天的日期
                        firstLoginDtState.update(DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000L));
                    } else {
                        // 留空
                        // 正常情况， 不需要修复
                    }
                } else {
                    // 当前数据is_new 不为 0  也不为 1   是错误数据
                }
                out.collect(value);
            }
        });
    }

    private static KeyedStream<JSONObject, String> keyByWithWatermark(SingleOutputStreamOperator<JSONObject> jsonObjStream) {
//        return jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3L))
//                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
//                    @Override
//                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
//                        return element.getLong("ts");
//                    }
//                })
//        ).keyBy(new KeySelector<JSONObject, String>() {
//            @Override
//            public String getKey(JSONObject value) throws Exception {
//                return value.getJSONObject("common").getString("mid");
//            }
//        });
        return jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                })).keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });
    }

    private static SingleOutputStreamOperator<JSONObject> elt(DataStreamSource<String> kafkaSource) {
        return kafkaSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    JSONObject page = jsonObject.getJSONObject("page");
                    JSONObject start = jsonObject.getJSONObject("start");
                    String ts = jsonObject.getString("ts");
                    JSONObject common = jsonObject.getJSONObject("common");

                    if (page != null || start != null ) {
//                        if (common!=null && ts!=null && common.getString("mid")!=null){
                            out.collect(jsonObject);
//                        }
                    }
                } catch (Exception e) {
//                    e.printStackTrace();
                    System.out.println("过滤掉脏数据：" + value);
                }
            }
        });
    }
}
