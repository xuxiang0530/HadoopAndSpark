package com.xuxiang.Core;

import com.univocity.parsers.annotations.Convert;
import com.xuxiang.model.ShopInfo;
import com.xuxiang.model.UserPay;
import com.xuxiang.model.UserView;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class ShopTotalTop10 extends LogAlikoubei{

    private static final Logger LOG = LoggerFactory.getLogger(ShopTotalTop10.class);

    public static void main(String[] args) {

        String payDataPath = "hdfs://bigdata:9000/xuxtest/data/user_pay/user_pay.txt";
        String viewDataPath = "hdfs://bigdata:9000/xuxtest/data/user_view/user_view.txt";
        String shopInfoDataPath = "hdfs://bigdata:9000/sqoopdata/xuxtest2/shop_info/d785aec5-b898-4693-8fea-3dc83f2469cf.txt";

        ShopTotalTop10 content = new ShopTotalTop10();
        if(args.length == 3) {
            content.runAlkoubei(args[0], args[1], args[2]);
        }
        else
        {
            content.runAlkoubei(shopInfoDataPath,payDataPath,viewDataPath);
        }
    }

    @Override
    protected void process(JavaRDD<ShopInfo> shopInfoJavaRDD, JavaRDD<UserPay> userPayJavaRDD, JavaRDD<UserView> userViewJavaRDD)
    {
        userPayJavaRDD.cache();
        shopInfoJavaRDD.cache();

        //分析数据
        //平均日交易额最大的前10个商家,输入他们交易额
        //需要知道商家,交易额,对user_pay的shop_id的数量进行求和,然后总数*人均消费
        //取出前十交易量的商家ID
        JavaPairRDD<String,Integer> shopPayNumber = userPayJavaRDD
                .mapToPair(pay -> new Tuple2<String,Integer>(pay.getShop_id(),1))
                .reduceByKey((v1,v2) ->(v1+ v2));

        JavaRDD<String>  top10 = zipShopInfoInteger(shopInfoJavaRDD,shopPayNumber)
                .mapToPair(x ->new Tuple2<Integer,ShopInfo>(x._2 * x._1.getPer_pay(),x._1))
                .sortByKey(false)
                .map(x-> (x._2.getShop_id()));

        top10.take(10).forEach(x -> System.out.println(x));
    }
}
