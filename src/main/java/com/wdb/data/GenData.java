package com.wdb.data;

import scala.Array;

import java.io.File;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by wangdabin1216 on 15/10/29.
 */
public class GenData {

    public static void main(String[] args) throws Exception{

        List<String> orderL = new ArrayList<String>();
        String act;
        //用户数据
        PrintWriter userTable = new PrintWriter(new File("/Users/wangdabin1216/work/lenovo/sbtSpark/data/hive/userTable"));
        for(int i = 0;i<3;i++) {
            if(i%2 ==0)
            userTable.println(i+"user" +"\t" + "\t" + "lenovo" +i +"\t" + System.currentTimeMillis());
            else
                userTable.println(i+"user" + "\t"  + "cookid" +i +"\t" + "\t" + System.currentTimeMillis());
        }

        //产品类别表
        String []pro = new String[]{"10","11"};

        //平台类型数据
        String []plt = new String[]{"11","10"};

        //平台表
        PrintWriter platTable = new PrintWriter(new File("/Users/wangdabin1216/work/lenovo/sbtSpark/data/hive/platTable"));
        for(int i = 0;i<2;i++) {
            String id = i+"pingtai";
            String type = plt[i%plt.length];
            String name = "name" + i;
            platTable.println(id + "\t" + type +"\t" + name);
        }

        //产品表
        PrintWriter productTable = new PrintWriter(new File("/Users/wangdabin1216/work/lenovo/sbtSpark/data/hive/productTable"));
        for(int i = 0;i<3;i++) {
            String id = i + "chanpin";
            String name = i + "name";
            String bussiness = "buss" + i;
            String type = pro[i%pro.length];
            String materialcode = "materialcode";
            String specification = "specification";
            String createtime = "1";
            productTable.println(id + "\t" + name + "\t" + bussiness  + "\t" + type + "\t" +materialcode + "\t" + specification + "\t" + createtime);
        }


        //商品数据
        List<String> ids = new ArrayList<String>();
        PrintWriter shops = new PrintWriter(new File("/Users/wangdabin1216/work/lenovo/sbtSpark/data/hive/shopTable"));
        int sum = 0;
        for(int i = 0;i<3;i++) {//3(产品号)
            for(int j = 0;j<2;j++){ //5个平台
                sum++;
                ids.add("shopid" + i + j);
                String id = "shopid" + i + j;
                String productid = i + "chanpin";
                String platformid = j + "pingtai";
                String code = i + j +"";
                String saletype = "" +( sum == 1 ?1:0);
                String price = "1";
                String marketable = "" +( sum == 2 ?0:1);
                String isgift = "" +( sum == 3 ?1:0);
                String istest ="" +( sum == 4 ?1:0);
                String createtime = "1111";

                shops.println( id + "\t" + productid+"\t" +platformid +"\t" + code + "\t" +saletype + "\t" + price + "\t" + marketable + "\t"   + isgift +"\t"  + istest+ "\t" + createtime);
            }
        }

        //行为表
        PrintWriter action = new PrintWriter(new File("/Users/wangdabin1216/work/lenovo/sbtSpark/data/hive/actionTable"));

        for(int i = 0;i<20;i++){//有1000个行为

            String id = i +"";
            String user1 =  ((int)(Math.random()*3) + 0) +"user";
            String type = "" +(i <5 ? 0 :1);
            String itemid = null;
            if("0".equals(type)){
                orderL.add(id+"dingdan");//保存订单标识
                itemid = id+"dingdan";
            }else{
                itemid = ids.get((int)(Math.random()*ids.size()) + 0);//为商品标识
            }

            String platformID = ((int)(Math.random()*10) + 0) + "pingtai";


            Date date = new SimpleDateFormat("yyyy-MM-dd").parse("1991-10-31");
            System.out.println(date.getTime());
            Date date1 = new SimpleDateFormat("yyyy-MM-dd").parse("1991-10-30");
            System.out.println(date1.getTime());
            Date date2 = new SimpleDateFormat("yyyy-MM-dd").parse("1991-10-29");
            System.out.println(date2.getTime());
            Date date3 = new SimpleDateFormat("yyyy-MM-dd").parse("1991-10-28");
            System.out.println(date3.getTime());
            List<Date> list = new ArrayList<Date>();
            list.add(date);
            list.add(date1);
            list.add(date2);
            list.add(date3);


            long actionTime = list.get((int) (Math.random() * list.size()) + 0).getTime();

            String date11 = new SimpleDateFormat("yyyyMMdd").format(new Date(actionTime));


            action.println(id  + "\t" + user1+"\t" +type + "\t" +itemid + "\t" + platformID + "\t" + actionTime + "\t" + date11);
        }

        System.out.println(orderL.size());
        //购品表
        PrintWriter purchase = new PrintWriter(new File("/Users/wangdabin1216/work/lenovo/sbtSpark/data/hive/purchaseTable"));
        for(int i = 0;i<orderL.size();i++) {
            for(int j = 0;j<(int)(Math.random()*1 + 1);j++) {
                String id = "gp" + i + "" + j;
                String orderId = orderL.get(i);
                String goodId = ids.get(((int) (Math.random() * ids.size()) + 0));
                String num = (int) (Math.random() * 5) + 0 + "";
                purchase.println(id + "\t" + orderId + "\t" + goodId + "\t" + num);
            }
        }

        userTable.close();
        action.close();
        productTable.close();
        shops.close();
        purchase.close();
        platTable.close();


    }

}
