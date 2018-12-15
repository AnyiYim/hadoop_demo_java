package com.mapreduce.userCF.step5;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Mapper5 extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();


    private List<String> cacheList = new ArrayList<String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        // 全局缓存 右侧矩阵 读入 List<String>
        FileReader fr = new FileReader("itemUserScore3");
        BufferedReader br = new BufferedReader(fr);
        // 每一行格式: 行/t列_值,列_值,..
        String line = null;
        while ((line = br.readLine()) != null) {
            cacheList.add(line);
        }
        fr.close();
        br.close();
    }

    /**
     *
     * @param key : 行
     * @param value : 行/t列_值,列_值,..
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String item_matrix1 = value.toString().split("\t")[0];
        String[] user_score_arr_matrix1 = value.toString().split("\t")[1].split(",");

        for (String line : cacheList) {
            // 右侧矩阵所有行列
            // value : 行/t列_值,列_值,..
            String item_matrix2 = line.toString().split("\t")[0];
            String[] user_score_arr_matrix2 = line.toString().split("\t")[1].split(",");
            // 如果行首的物品id相同 进行操作
            if (item_matrix1.equals(item_matrix2)) {
                for (String user_score_matrix1 : user_score_arr_matrix1) {
                    boolean flag = false;
                    String user_matrix1 = user_score_matrix1.split("_")[0];
                    String score_matrix1 = user_score_matrix1.split("_")[1];
                    for (String user_score_matrix2 : user_score_arr_matrix2) {
                        String user_matrix2 = user_score_matrix2.split("_")[0];
                        if (user_matrix1.equals(user_matrix2)) {
                            flag = true;
                        }
                    }
                    if (!flag) {
                        outKey.set(item_matrix1);
                        outValue.set(user_matrix1 + "_" + score_matrix1);
                        context.write(outKey, outValue);
                    }
                }
            }
        }
    }
}
