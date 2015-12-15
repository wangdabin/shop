package com.wdb.data;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by wangdabin1216 on 15/11/4.
 */
public class Test {
    public static void main(String[] args) throws Exception {
        Date date = new SimpleDateFormat("yyyy-MM-dd").parse("1991-10-31");
        System.out.println(date.getTime());

        Date date1 = new SimpleDateFormat("yyyy-MM-dd").parse("1991-09-22");
        System.out.println(date1.getTime());
        Date date2 = new SimpleDateFormat("yyyy-MM-dd").parse("1991-12-31");

        System.out.println(date2.getTime());
        Date date3 = new SimpleDateFormat("yyyy-MM-dd").parse("1991-08-31");
        System.out.println(date3.getTime());
    }

}
