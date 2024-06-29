package com.atguigu.gmall.realtime.dws.function;

import com.atguigu.gmall.realtime.common.util.IkUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @author name 婉然从物
 * @create 2024-04-28 9:29
 */
public class KwSplit extends TableFunction<Row> {

    @FunctionHint(output = @DataTypeHint("ROW<keyword STRING>"))
    public void eval(String keywords) {
        List<String> stringList = IkUtil.IKSplit(keywords);
        for (String s : stringList) {
            collect(Row.of(s));
        }
    }
}