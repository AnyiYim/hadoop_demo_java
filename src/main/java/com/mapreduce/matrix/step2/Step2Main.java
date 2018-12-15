package com.mapreduce.matrix.step2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class Step2Main {
    private static String inPath = "/matrix/step2_input/matrix1.txt";
    private static String outPath = "/matrix/output";
    private static String cache = "/matrix/step1_output/part-r-00000";
    private static String hdfs = "hdfs://localhost:9000";

    public int run() {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", hdfs);
            Job job = Job.getInstance(conf, "step2");

            job.addCacheArchive(new URI(cache + "#matrix2"));

            job.setJarByClass(Step2Main.class);
            job.setMapperClass(Step2Mapper.class);
            job.setReducerClass(Step2Reducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileSystem fs = FileSystem.get(conf);
            Path inputPath = new Path(inPath);
            if (fs.exists(inputPath)) {
                FileInputFormat.addInputPath(job, inputPath);
            }
            Path outputPath = new Path(outPath);
            fs.delete(outputPath, true);

            FileOutputFormat.setOutputPath(job, outputPath);

            return job.waitForCompletion(true)? 1: -1;



        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public static void main(String[] args) {
        int result = -1;
        result = new Step2Main().run();
        if (result == 1) {
            System.out.println("step2运行成功...");
        }
        else if (result == -1) {
            System.out.println("step2运行失败...");
        }
    }

}
