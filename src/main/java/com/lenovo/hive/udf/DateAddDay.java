package com.lenovo.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * Created by wangdabin1216 on 15/11/16.
 */
public class DateAddDay extends UDF {
    public String evaluate(String time, Integer days) throws Exception {
        if (time == null || days == null) {
            return null;
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date date = sdf.parse(time);
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.DATE, days);
        String result = sdf.format(c.getTime());
        return result;
    }

    public static void main(String[] args)throws Exception{
        DateAddDay dateAddDay =  new DateAddDay();
        System.out.printf(dateAddDay.evaluate("19911031",-1));
    }

}
