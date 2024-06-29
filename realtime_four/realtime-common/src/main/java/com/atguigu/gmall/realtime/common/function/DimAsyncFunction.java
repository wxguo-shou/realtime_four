package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author name 婉然从物
 * @create 2024-06-23 0:16
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T>{
    StatefulRedisConnection<String, String> redisAsyncConnection;
    AsyncConnection hBaseAsyncConnection;

    @Override
    public void open(Configuration parameters) throws Exception {
        redisAsyncConnection = RedisUtil.getRedisAsyncConnection();
        hBaseAsyncConnection = HBaseUtil.getHBaseAsyncConnection();
    }

    @Override
    public void close() throws Exception {
        RedisUtil.closeRedisAsyncConnection(redisAsyncConnection);
        HBaseUtil.closeAsyncHbaseConnection(hBaseAsyncConnection);
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        // Java的异步编程方式
        String tableName = getTableName();
        String rowKey = getId(input);
        String redisKey = RedisUtil.getRedisKey(tableName, rowKey);
        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                // 第一步异步访问得到的数据
                RedisFuture<String> dimSkuInfoFuture = redisAsyncConnection.async().get(redisKey);
                String dimInfo = null;
                try {
                    dimInfo = dimSkuInfoFuture.get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return dimInfo;
            }
        }).thenApplyAsync(new Function<String, JSONObject>() {
            @Override
            public JSONObject apply(String dimInfo) {
                JSONObject dimJsonObj = null;
                // 旁路缓存判断
                if (dimInfo == null || dimInfo.length() == 0) {
                    // 需要访问HBase
                    try {
                        dimJsonObj = HBaseUtil.getAsyncCells(hBaseAsyncConnection, Constant.HBASE_NAMESPACE, tableName, rowKey);
                        // 将读取的数据保存到redis
                        redisAsyncConnection.async().setex(redisKey, 24 * 60 * 60, dimJsonObj.toJSONString());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    // redis中存在缓存数据
                    dimJsonObj = JSONObject.parseObject(dimInfo);
                }
                return dimJsonObj;
            }
        }).thenAccept(new Consumer<JSONObject>() {
            @Override
            public void accept(JSONObject dim) {
                // 合并维度信息
                if (dim == null) {
                    // 无法关联到维度信息
                    System.out.println("无法关联当前的维度信息" + tableName + ":" + rowKey);
                } else {
                    join(input, dim);
                }
                // 返回结果
                resultFuture.complete(Collections.singletonList(input));
            }
        });

    }
}

