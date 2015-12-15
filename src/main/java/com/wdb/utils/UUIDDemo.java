package com.wdb.utils;

/**
 * Created by wangdabin1216 on 15/11/9.
 */
import java.text.SimpleDateFormat;
import java.util.*;

public class UUIDDemo {
    public static void main(String[] args) throws Exception {
        String time = "19911011";
        int days = 1;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date date = sdf.parse(time);
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.DATE, -days);
        System.out.println(sdf.format(c.getTime()));

        String startTime = "19911011";
        String endTime = "19911010";
        Date start = sdf.parse(startTime);
        Date end = sdf.parse(endTime);
        long diffInMilliSeconds = start.getTime() - end.getTime();
        int days1 = (int) (diffInMilliSeconds / (86400 * 1000));
        System.out.println(days1);
    }
}