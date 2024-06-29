package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TradeSkuOrderBean;

/**
 * @author name 婉然从物
 * @create 2024-06-23 0:54
 */
public interface DimJoinFunction<T> {
    public String getId(T input);
    public String getTableName();
    public void join(T input, JSONObject dim);
}
