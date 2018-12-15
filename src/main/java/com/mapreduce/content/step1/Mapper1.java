package com.mapreduce.content.step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    /**
     * key : 1
     * value : 1	1_0,2_3,3_-1,4_2,5_-3
     */

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] rowAndLine = value.toString().split("\t");
        // 矩阵行号
        String row = rowAndLine[0];
        String[] lines  = rowAndLine[1].split(",");
        for (String line : lines) {
            // 列 和 值
            String column  = line.split("_")[0];
            String valueStr = line.split("_")[1];
            // key: 列   value: 行号_值     转置
            outKey.set(column);
            outValue.set(row + "_" + valueStr);
            context.write(outKey, outValue);
        }

    }
}
