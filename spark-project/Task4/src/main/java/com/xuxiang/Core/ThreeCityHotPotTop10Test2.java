package com.xuxiang.Core;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.xuxiang.model.ShopInfo;
import com.xuxiang.model.UserPay;
import com.xuxiang.model.UserView;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ThreeCityHotPotTop10Test2 extends LogAlikoubei {
    private static final Logger LOG = LoggerFactory.getLogger(ShopTotalTop10.class);

    public static void main(String[] args) {
        String payDataPath = "/home/bigdata/xuxtestdata/dataset/user_pay.txt";
        String viewDataPath = "/home/bigdata/xuxtestdata/dataset/user_view.txt";
        String shopInfoDataPath = "/home/bigdata/xuxtestdata/dataset/shop_info.txt";

//        String payDataPath = "hdfs://bigdata:9000/xuxtest/data/user_pay/user_pay.txt";
//        String viewDataPath = "hdfs://bigdata:9000/xuxtest/data/user_view/user_view.txt";
//        String shopInfoDataPath = "hdfs://bigdata:9000/sqoopdata/xuxtest2/shop_info/d785aec5-b898-4693-8fea-3dc83f2469cf.txt";

        ThreeCityHotPotTop10Test2 ThreeCityHotPotTop10Test2 = new ThreeCityHotPotTop10Test2();
        if(args.length == 3) {
            ThreeCityHotPotTop10Test2.runAlkoubei(args[0], args[1], args[2]);
        }
        else
        {
            ThreeCityHotPotTop10Test2.runAlkoubei(shopInfoDataPath,payDataPath,viewDataPath);
        }
    }

    @Override
    protected void process(JavaRDD<ShopInfo> shopInfoJavaRDD, JavaRDD<UserPay> userPayJavaRDD, JavaRDD<UserView> userViewJavaRDD)
    {
        userPayJavaRDD.cache();
        shopInfoJavaRDD.cache();

//        shopInfoJavaRDD.foreachPartition(x -> x.forEachRemaining(y-> System.out.println(y)));
//        userPayJavaRDD.foreachPartition(x -> x.forEachRemaining(y-> System.out.println(y)));
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
        //HotPotTreeCity.foreach(x -> System.out.println(x.getShop_id() + "::check:1"));

        List<ShopInfo> HotPotList = HotPotTreeCity.collect();

        ArrayList<String> LS = new ArrayList<String>();/* = new List; */
        HotPotList.forEach(x -> LS.add(x.getShop_id()));

        Broadcast<List<String>> HotPotBroad = createBroadcast(LS);

        //获得所有火锅店的消费记录
        JavaRDD<UserPay>  HotPotUserPay = userPayJavaRDD
                .filter(x -> HotPotBroad.value().contains(x.getShop_id()));

//        HotPotUserPay.foreach(x -> System.out.println(x.getShop_id() + "::check:2"));

        //获得3个城市火锅店的消费RDD//经过测试,发现类作为key值时,不能很好的reduceByKey,更改标识为ShopId
        //JavaPairRDD<ShopInfo,String> HotPotTreeCityPayDay = zipShopInfoString(HotPotTreeCity,HotPotUserPay);

        //获得火锅店每日消费总次数
//        JavaPairRDD<Tuple2<ShopInfo,String>,Integer> HotPotTreeCityPayDayCount = HotPotTreeCityPayDay
//                .mapToPair(x-> new Tuple2<Tuple2<ShopInfo,String>,Integer>(new Tuple2<ShopInfo,String>(x._1,x._2),1))
//                .reduceByKey((v1,v2) -> (v1 + v2))
//                .mapToPair(x -> new Tuple2<Tuple2<ShopInfo,String>,Integer>(new Tuple2<ShopInfo,String>(x._1._1,x._1._2),x._2 * x._1._1.getPer_pay()));
        JavaPairRDD<Tuple2<String,String>,Integer> HotPotTreeCityPayDayCount = HotPotUserPay
                .mapToPair(x-> new Tuple2<Tuple2<String,String>,Integer>(new Tuple2<String,String>(x.getShop_id(),x.getTime_stamp()),1))
                .reduceByKey((v1,v2) -> (v1 + v2));
        //System.out.println(String.format("%s,%s,第几个测试点:%d",HotPotTreeCityPayDayCount.count(),"HotPotTreeCityPayDayCount",1));
        HotPotTreeCityPayDayCount.foreach(x -> System.out.println(x._1 + "::" + x._2));


        HotPotTreeCityPayDayCount.cache();
        //HotPotTreeCityPayDayCount.foreach(x -> System.out.println(String.format("%s,%s,%d",x._1._1.getShop_id(),x._1._2,x._2) + "::check:2"));
        //获得火锅店每日消费最高次数
        JavaPairRDD<String,Integer> HotPotTreeCityPayDayMax = HotPotTreeCityPayDayCount
                .mapToPair(x -> new Tuple2<String,Integer>(x._1._1,x._2))
                .reduceByKey((v1,v2) -> (Math.max(v1,v2)));

//        System.out.println(String.format("%s,%s,第几个测试点:%d",HotPotTreeCityPayDayMax.count(),"HotPotTreeCityPayDayCount,应该和火锅店数量一致",2));
//        HotPotTreeCityPayDayMax.foreach(x -> System.out.println(x._1 + "::" + x._2));

        //获得火锅店消费总次数
        JavaPairRDD<String,Integer> HotPotTreeCityPayCount = HotPotTreeCityPayDayCount
                .mapToPair(x-> new Tuple2<String,Integer>(x._1._1,x._2))
                .reduceByKey((v1,v2) -> (v1 + v2));

//        System.out.println(String.format("%s,%s,第几个测试点:%d",HotPotTreeCityPayCount.count(),"HotPotTreeCityPayCount,应该和火锅店数量一致",3));
//        HotPotTreeCityPayCount.foreach(x -> System.out.println(x._1 + "::" + x._2));


        //获得有消费的日子
        JavaPairRDD<String,Integer> HotPotTreeCityPayDays = HotPotTreeCityPayDayCount
                .mapToPair(x -> new Tuple2<String,Integer>(x._1._1,1))
                .reduceByKey((v1,v2) -> (v1+v2));


        //火锅店消费次数重新方法
        JavaPairRDD<String,Iterable<String>>HotPotShopPay = HotPotUserPay
                .mapToPair(x -> new Tuple2<String,String>(x.getShop_id(),x.getTime_stamp()))
                .groupByKey();

        //获得火锅店每日消费最高次数
        JavaPairRDD<String,Integer> HotPotTreeCityPayDayMax2 = HotPotTreeCityPayDayCount
                .mapToPair(x -> new Tuple2<String,Integer>(x._1._1,x._2))
                .reduceByKey((v1,v2) -> (Math.max(v1,v2)));

        JavaPairRDD<String,ShopInfo> StringShop = HotPotTreeCity
                .mapToPair(x -> new Tuple2<String,ShopInfo>(x.getShop_id(),x));

        //评分2
        //最受欢迎为(0.7* ( 平均评分 / 5) + 0.3 * (平均消费金额 / 最高消费金额) )
        JavaPairRDD<Double,String> HotPotTreeCityScore2 = HotPotShopPay
                .join(StringShop)
                .join(HotPotTreeCityPayDayMax2)
                .mapToPair(x -> {
                    Double pf = 0.0;
                    int cs = Iterators.size(x._2._1._1.iterator());
                    int days = Sets.newHashSet(x._2._1._1).size();
                    String ShopId = x._1;
                    pf =  (0.7* ( x._2._1._2.getPer_pay() / 5) + 0.3 * ((cs * x._2._1._2.getPer_pay() / days) / (x._2._2 * x._2._1._2.getPer_pay())));
                    return new Tuple2<Double,String>(pf, ShopId);
                });

//        System.out.println(String.format("%s,%s,第几个测试点:%d",HotPotTreeCityPayDays.count(),"HotPotTreeCityPayDays,应该和火锅店数量一致",4));
//        HotPotTreeCityPayDays.foreach(x -> System.out.println(x._1 + "::" + x._2));
        //火锅店和评分
        JavaPairRDD<Double,String> HotPotTreeCityScore = LikeShop(HotPotTreeCityPayDayMax,HotPotTreeCityPayCount,HotPotTreeCityPayDays,HotPotTreeCity);

        HotPotTreeCityScore.foreachPartition(x->x.forEachRemaining(y->System.out.println(y._1)));
        //排序输出
        HotPotTreeCityScore.sortByKey(false).take(10).forEach(x -> System.out.println(x._2));
    }


}
