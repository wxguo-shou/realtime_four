package com.atguigu.gmall.realtime.common.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author name 婉然从物
 * @create 2024-04-27 10:49
 */
public class IkUtil {

    public static List<String> IKSplit(String keywords){
        StringReader stringReader = new StringReader(keywords);
        IKSegmenter ikSegmenter = new IKSegmenter(stringReader, true);
        List<String> result = new ArrayList<>();
        try {
            Lexeme next = ikSegmenter.next();
            while (next != null) {
                result.add(next.getLexemeText());
                next = ikSegmenter.next();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

}
