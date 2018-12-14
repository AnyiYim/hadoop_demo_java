package com.mapreduce.itemCF_job;

import com.mapreduce.itemCF_step1.Main1;
import com.mapreduce.itemCF_step2.Main2;
import com.mapreduce.itemCF_step3.Main3;
import com.mapreduce.itemCF_step4.Main4;
import com.mapreduce.itemCF_step5.Main5;

public class JobRunner {
    public static void main(String[] args) {
        int status1 = -1;
        int status2 = -1;
        int status3 = -1;
        int status4 = -1;
        int status5 = -1;

        status1 = new Main1().run();
        if (status1 == 1) {
            System.out.println("Step1 运行成功! 正在运行 Step2 ...");
            status2 = new Main2().run();
        }
        else {
            System.out.println("Step1 运行失败.");
        }
        if (status2 == 1) {
            System.out.println("Step2 运行成功! 正在运行 Step3 ...");
            status3 = new Main3().run();
        }
        else {
            System.out.println("Step2 运行失败.");
        }
        if (status3 == 1) {
            System.out.println("Step3 运行成功! 正在运行 Step4 ...");
            status4 = new Main4().run();
        }
        else {
            System.out.println("Step3 运行失败.");
        }
        if (status4 == 1) {
            System.out.println("Step4 运行成功! 正在运行 Step5 ...");
            status5 = new Main5().run();
        }
        else {
            System.out.println("Step4 运行失败.");
        }
        if (status5 == 1) {
            System.out.println("Step5 运行成功! 程序结束");
        }
        else {
            System.out.println("Step5 运行失败.");
        }
    }
}
