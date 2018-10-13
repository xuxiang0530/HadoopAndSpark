package com.xuxiang.Core;

import com.xuxiang.model.ShopInfo;
import com.xuxiang.model.UserPay;
import com.xuxiang.model.UserView;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;

import java.util.List;

public class ThreeCityHotPotTop10 extends LogAlikoubei{

    private static final Logger LOG = LoggerFactory.getLogger(ShopTotalTop10.class);

    public static void main(String[] args) {
        System.out.println(0.6+0.5);
//
//        String payDataPath = "hdfs://bigdata:9000/xuxtest/data/user_pay/user_pay.txt";
//        String viewDataPath = "hdfs://bigdata:9000/xuxtest/data/user_view/user_view.txt";
//        String shopInfoDataPath = "hdfs://bigdata:9000/sqoopdata/xuxtest2/shop_info/d785aec5-b898-4693-8fea-3dc83f2469cf.txt";
//
//        ThreeCityHotPotTop10 CityHotPotTop10 = new ThreeCityHotPotTop10();
//        if(args.length == 3) {
//            CityHotPotTop10.runAlkoubei(args[0], args[1], args[2]);
//        }
//        else
//        {
//            CityHotPotTop10.runAlkoubei(shopInfoDataPath,payDataPath,viewDataPath);
//        }
    }

    @Override
    protected void process(JavaRDD<ShopInfo> shopInfoJavaRDD, JavaRDD<UserPay> userPayJavaRDD, JavaRDD<UserView> userViewJavaRDD)
    {
        userPayJavaRDD.cache();
        shopInfoJavaRDD.cache();

        //分别输出北京\上海.广州3个城市最受欢迎的10家火锅商店编号
        //最受欢迎为(0.7* ( 平均评分 / 5) + 0.3 * (平均消费金额 / 最高消费金额) )
        //因为只有一个人均消费,和消费次数,所以,
        // 最高消费金额定义为,MAX(单日消费次数 * 人均消费)
        // 平均消费金额定义为,总消费次数 * 人均消费 / 有消费的天数
        //简约算法可以为平价每日次数/最高每日次数

        Broadcast<List<String>> cityBroad = createBroadcast(Arrays.asList("北京","上海","广州"));

        JavaRDD<ShopInfo> HotPotTreeCity = shopInfoJavaRDD
                .filter(x -> (x.isHotPot() && cityBroad.value().contains(x.getCity_name())));

        HotPotTreeCity.cache();
        //1号检查点,检查是否获得了3个城市的火锅
        HotPotTreeCity.foreach(x -> System.out.println(x.getShop_id() + "::check:1"));

        List<ShopInfo> HotPotList = HotPotTreeCity.collect();

        ArrayList<String> LS = new ArrayList<String>();/* = new List; */
         HotPotList.forEach(x -> LS.add(x.getShop_id()));

        Broadcast<List<String>> HotPotBroad = createBroadcast(LS);

        //获得所有火锅店的消费记录
        JavaRDD<UserPay>  HotPotUserPay = userPayJavaRDD
                .filter(x -> HotPotBroad.value().contains(x.getShop_id()));

        //获得3个城市火锅店的消费RDD
        JavaPairRDD<ShopInfo,String> HotPotTreeCityPayDay = zipShopInfoString(HotPotTreeCity,HotPotUserPay);

        //获得火锅店每日消费总次数
        JavaPairRDD<Tuple2<ShopInfo,String>,Integer> HotPotTreeCityPayDayCount = HotPotTreeCityPayDay
                .mapToPair(x-> new Tuple2<Tuple2<ShopInfo,String>,Integer>(new Tuple2<ShopInfo,String>(x._1,x._2),1))
                .reduceByKey((v1,v2) -> (v1 + v2))
                .mapToPair(x -> new Tuple2<Tuple2<ShopInfo,String>,Integer>(new Tuple2<ShopInfo,String>(x._1._1,x._1._2),x._2 * x._1._1.getPer_pay()));

        HotPotTreeCityPayDayCount.cache();
        //获得火锅店每日消费最高次数
        JavaPairRDD<ShopInfo,Integer> HotPotTreeCityPayDayMax = HotPotTreeCityPayDayCount
                .mapToPair(x -> new Tuple2<ShopInfo,Integer>(x._1._1,x._2))
                .reduceByKey((v1,v2) -> (Math.max(v1,v2)));

        //获得火锅店消费总次数
        JavaPairRDD<ShopInfo,Integer> HotPotTreeCityPayCount = HotPotTreeCityPayDayCount
                .mapToPair(x-> new Tuple2<ShopInfo,Integer>(x._1._1,x._2))
                .reduceByKey((v1,v2) -> (v1 + v2));

        //获得有消费的日子
        JavaPairRDD<ShopInfo,Integer> HotPotTreeCityPayDays = HotPotTreeCityPayDayCount
                .mapToPair(x -> new Tuple2<ShopInfo,Integer>(x._1._1,1))
                .reduceByKey((v1,v2) -> (v1+v2));

        //火锅店和评分
//        JavaPairRDD<Double,ShopInfo> HotPotTreeCityScore = LikeShop(HotPotTreeCityPayDayMax,HotPotTreeCityPayCount,HotPotTreeCityPayDays);

        //排序输出
//        HotPotTreeCityScore.sortByKey(false).take(10).forEach(x -> System.out.println(x._2.getShop_id()));
    }




    //sql方法
//    protected void process(JavaRDD<ShopInfo> shopInfoJavaRDD, JavaRDD<UserPay> userPayJavaRDD, JavaRDD<UserView> userViewJavaRDD) {
//        Dataset<Row> userPayDF = toPayDataFrame(userPayJavaRDD);
//        Dataset<Row> shopInfoDF = toShopDataFrame(shopInfoJavaRDD);
//        //Dataset<Row> userViewDF = toViewDataFrame(userViewJavaRDD);
//        userPayDF.cache();
//        shopInfoDF.cache();
//        //userViewDF.cache();
//        userPayDF.createOrReplaceTempView("pays");
//        shopInfoDF.createOrReplaceTempView("shopInfo");
//        //userViewDF.createOrReplaceTempView("views");
//        String sqlstr = "SELECT C.shop_id, (0.7* ( D.score / 5) + 0.3 * ((D.per_pay * D.payTotal / days) / (D.per_pay * D.oneDayMax)) )\n" +
//                "FROM \n" +
//                "(\tSELECT shop_id,COUNT(DAY) days,sum(oneDayTotal) payTotal,max(oneDayTotal) oneDayMax\n" +
//                "\tFROM \n" +
//                "\t(\n" +
//                "\t\tSELECT shop_id,DAY,COUNT(shop_id) oneDayTotal\n" +
//                "\t\tFROM\n" +
//                "\t\t\t(\n" +
//                "\t\t\tSELECT shop_id,toDayStr(time_stamp) DAY\n" +
//                "\t\t\tFROM pays \n" +
//                "\t\t\tWHERE EXISTS (SELECT shop_id FROM shopInfo WHERE cate_1_name = '火锅' AND city_name IN ('上海','广州','北京'))\n" +
//                "\t\t\t) A\n" +
//                "\t\tGROUP BY shop_id,DAY\n" +
//                "\t) B\n" +
//                "\tGROUP BY B.shop_id\n" +
//                ") C\n" +
//                "join\n" +
//                "shopInfo D\n" +
//                "on C.shop_id = D.shop_id";
//
//        /*
//SELECT C.shop_id, (0.7* ( D.score / 5) + 0.3 * ((D.per_pay * D.payTotal / days) / (D.per_pay * D.oneDayMax)) )
//FROM
//(	SELECT shop_id,COUNT(DAY) days,sum(oneDayTotal) payTotal,max(oneDayTotal) oneDayMax
//	FROM
//	(
//		SELECT shop_id,DAY,COUNT(shop_id) oneDayTotal
//		FROM
//			(
//			SELECT shop_id,toDayStr(time_stamp) DAY
//			FROM pays
//			WHERE EXISTS (SELECT shop_id FROM shopInfo WHERE cate_1_name = '火锅' AND city_name IN ('上海','广州','北京'))
//			) A
//		GROUP BY shop_id,DAY
//	) B
//	GROUP BY B.shop_id
//) C
//join
//shopInfo D
//on C.shop_id = D.shop_id
//         */
//
//
//        Dataset<Row> contentCounts = spark.sql(sqlstr);
//
//        contentCounts.foreachPartition(rows -> {
//            //Connection conn = DBHelper.getConnection();
//            rows.forEachRemaining(row -> {
//                try {
//                    //JavaDBDao.saveContentCount(conn, row.getLong(0), row.getString(1), row.getLong(2), row.getLong(3));
//                    System.out.print(row.getString(0));
//                    //} catch (SQLException e) {
//                } catch (Exception e) {
//                    LOG.error("Some error", e);
//                }
//            });
//            //conn.close();
//        });
//
//    }

}
