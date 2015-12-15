package com.lenovo.hive.udf;

/**
 * Created by wangdabin1216 on 15/11/16.
 */
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.DoubleWritable;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class WeightSumScore extends UDF {
    //返回对应的评分
//    public  Double evaluate(String startTime,String endTime,Double score,Integer limit) throws Exception{
//
//        if (startTime == null || endTime == null || score == null || limit == null) {
//            return null;
//        }
//        // NOTE: This implementation avoids the extra-second problem
//        // by comparing with UTC epoch and integer division.
//        // 86400 is the number of seconds in a day
//
//        //1.计算时间差
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
//        Date start = sdf.parse(startTime);
//        Date end = sdf.parse(endTime);
//        long diffInMilliSeconds = start.getTime() - end.getTime();
//        int days = (int) (diffInMilliSeconds / (86400 * 1000));
//        if(days > limit){
//            days = limit;
//        }
//        //2.计算对应的得分根据公式
//        Double score_fin = (1-(days*days*1.0)/(limit*limit)) * score;
//        DecimalFormat df = new DecimalFormat("#0.##");
//        score_fin = Double.valueOf(df.format(score_fin));
//        return score_fin;
//    }
    public  Double evaluate(String startTime,String endTime,Integer limit) throws Exception{

        if (startTime == null || endTime == null || limit == null) {
            throw new Exception(startTime + "" + endTime + "" + limit);
        }
        // NOTE: This implementation avoids the extra-second problem
        // by comparing with UTC epoch and integer division.
        // 86400 is the number of seconds in a day

        //1.计算时间差
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date start = sdf.parse(startTime);
        Date end = sdf.parse(endTime);
        long diffInMilliSeconds = start.getTime() - end.getTime();

        int days = (int) (diffInMilliSeconds / (86400 * 1000));
        if(days > limit){
            days = limit;
        }
        //2.计算对应的得分根据公式
        Double weight = (1-(days*days*1.0)/(limit*limit));
//        DecimalFormat df = new DecimalFormat("#0.##");
//        weight = Double.valueOf(df.format(weight));

        DecimalFormat formater = new DecimalFormat();
        formater.setMaximumFractionDigits(2);
        formater.setGroupingSize(0);
        formater.setRoundingMode(RoundingMode.FLOOR);
        weight =Double.valueOf(formater.format(weight));

        if(weight == null){
            throw new Exception(startTime + "" + endTime + "" + limit);
        }
        return weight;
    }

//    public static void main(String[] args) throws Exception {
//        System.out.println(new WeightSumScore().evaluate("20151130", "20151128", 90));
//    }
}