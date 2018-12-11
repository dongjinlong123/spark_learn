package com.djl.hive;


import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
/**
 * 自定义函数UDF
 */
public class MyHiveUDF extends UDF {
    /**
     * 参数转化为小写
     * @param s
     * @return
     */
    public Text evaluate(final Text s) {
        if (s == null) { return null; }
        return new Text(s.toString().toLowerCase());
    }
}
