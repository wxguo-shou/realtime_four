package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.atguigu.gmall.realtime.common.bean.TrafficPageViewBean;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author name 婉然从物
 * @create 2024-05-06 19:19
 */
public class DorisMapFunction<T> implements MapFunction<T, String> {
    @Override
    public String map(T value) throws Exception {
        SerializeConfig config = new SerializeConfig();
        config.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        return JSONObject.toJSONString(value, config);
    }
}
