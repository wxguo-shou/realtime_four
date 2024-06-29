package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author name 婉然从物
 * @create 2024-06-02 18:29
 */
public class test extends BaseApp {
    public static void main(String[] args) {
        new test().start(10024, 4, "dws_traffic_home_detail_page_view_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 核心业务逻辑
        // TODO 1、读取DWD层page主题
        stream.print();

//        // TODO 2、清洗过滤数据
//        SingleOutputStreamOperator<JSONObject> jsonObjStream = etl(stream);
//
//        // TODO 3、按照mid分组
//        KeyedStream<JSONObject, String> keyedStream = getKeyedStram(jsonObjStream);
//
//        // TODO 4、判断独立访客
//        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> processBeanStream = uvCountBean(keyedStream);
//
////        processBeanStream.print();
//
//        // TODO 5、添加水位线
//        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> withWatermarkStream = getWithWatermarkStream(processBeanStream);
//
//        // TODO 6、分组开窗聚合
//        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduceStream = windowAndAgg(withWatermarkStream);
//
////        reduceStream.print();
//
//        // TODO 7、写出到Doris
//        reduceStream.map(new DorisMapFunction<>())
//                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW));
    }

    private static SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> windowAndAgg(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> withWatermarkStream) {
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduceStream = withWatermarkStream.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                        // 将度量值合并到一起
                        value1.setHomeUvCt(value1.getHomeUvCt() + value1.getHomeUvCt());
                        value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                        return value1;
                    }
                }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> values, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                        for (TrafficHomeDetailPageViewBean value : values) {
                            value.setStt(stt);
                            value.setEdt(edt);
                            value.setCurDate(curDt);
                            out.collect(value);
                        }
                    }
                });
        return reduceStream;
    }

    private static SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> getWithWatermarkStream(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> processBeanStream) {
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> withWatermarkStream = processBeanStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<TrafficHomeDetailPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(
                        new SerializableTimestampAssigner<TrafficHomeDetailPageViewBean>() {
                            @Override
                            public long extractTimestamp(TrafficHomeDetailPageViewBean element, long recordTimestamp) {
                                return element.getTs();
                            }
                        }
                ));
        return withWatermarkStream;
    }

    private static SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> uvCountBean(KeyedStream<JSONObject, String> keyedStream) {
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> processBeanStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {
            ValueState<String> homeLastLoginState;
            ValueState<String> detailLastLoginState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> homeLastLoginDesc = new ValueStateDescriptor<>("home_last_login", String.class);
                homeLastLoginDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1L)).build());
                homeLastLoginState = getRuntimeContext().getState(homeLastLoginDesc);

                ValueStateDescriptor<String> detailLastLoginDesc = new ValueStateDescriptor<>("detail_last_login", String.class);
                detailLastLoginDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1L)).build());
                detailLastLoginState = getRuntimeContext().getState(detailLastLoginDesc);
            }

            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>.Context ctx, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                String pageId = value.getJSONObject("page").getString("page_id");
                Long ts = value.getLong("ts");
                String curDt = DateFormatUtil.tsToDate(ts);
                // 首页独立访客数
                Long homeUvCt = 0L;
                // 商品详情页独立访客数
                Long goodDetailUvCt = 0L;

                if ("home".equals(pageId)) {
                    String homeLastLoginDt = homeLastLoginState.value();
                    if (homeLastLoginDt == null || !homeLastLoginDt.equals(curDt)) {
                        // 首页的独立访客
                        homeUvCt = 1L;
                        homeLastLoginState.update(curDt);
                    }
                } else {
                    // 商品详情页
                    String detailLastLoginDt = detailLastLoginState.value();
                    if (detailLastLoginDt == null || !detailLastLoginDt.equals(curDt)) {
                        goodDetailUvCt = 1L;
                        detailLastLoginState.update(curDt);
                    }
                }
                // 如果两个独立访客的度量值都为0  可以过滤掉   不需要往下游发送
                if (homeUvCt + goodDetailUvCt > 0) {
                    out.collect(TrafficHomeDetailPageViewBean.builder()
                            .homeUvCt(homeUvCt)
                            .goodDetailUvCt(goodDetailUvCt)
                            .ts(ts)
                            .build());
                }
            }
        });
        return processBeanStream;
    }}

