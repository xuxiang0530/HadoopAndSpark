package com.xuxiang.Core;

import com.alibaba.fastjson.JSON;
import com.univocity.parsers.annotations.Convert;
import com.xuxiang.model.ShopInfo;
import com.xuxiang.model.UserPay;
import com.xuxiang.model.UserView;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

abstract class LogAlikoubei{

        final protected JavaSparkContext jsc;
        final protected SparkSession spark;
        final protected SparkConf conf;

        public LogAlikoubei() {
            conf = new SparkConf();
            conf.setIfMissing("spark.app.name", getClass().getSimpleName());
            spark = SparkSession.builder().config(conf).getOrCreate();
            jsc = new JavaSparkContext(spark.sparkContext());
        }

    protected Broadcast<List<String>> createBroadcast(List<String> Lstr)
    {
        return jsc.broadcast(Lstr);
    }

        //读取shop文件
        private JavaRDD<ShopInfo> parseShopInfoLogFile(String ShopInfopath) {
            JavaRDD<String> lines = jsc.textFile(ShopInfopath);
    //            JavaRDD<ShopInfo> ShopInfos = lines.filter(x -> x.split(",").length == 10).map(line ->{
    //                line = line + "qita";
    //                return JSON.parseObject(line, ShopInfo.class);
    //            });

            JavaRDD<ShopInfo> ShopInfos = lines.map(str -> {
                String[] strArray = str.split(",");
                ShopInfo shopInfo = new ShopInfo();
                shopInfo.setShop_id(strArray[0]);
                shopInfo.setCity_name(strArray[1]);
                shopInfo.setLocation_id(strArray[2]);
                shopInfo.setPer_pay(strArray[3]);
                shopInfo.setScore(strArray[4]);
                shopInfo.setComment_cnt(strArray[5]);
                shopInfo.setShop_level(strArray[6]);
                shopInfo.setCate_1_name(strArray[7]);
                shopInfo.setCate_2_name(strArray[8]);
                if (strArray.length == 9)
                    shopInfo.setCate_3_name("");
                else
                    shopInfo.setCate_3_name(strArray[9]);
                return shopInfo;
            });
            return ShopInfos;
        }

        //读取pay文件
        private JavaRDD<UserPay> parsePayLogFile(String paypath) {
            JavaRDD<String> lines = jsc.textFile(paypath);
            //JavaRDD<UserPay> paylogs = lines.map(line -> JSON.parseObject(line, UserPay.class));
            JavaRDD<UserPay> paylogs = lines.map(x -> {
                String[] str = x.split(",");
                UserPay up = new UserPay();
                up.setUser_id(str[0]);
                up.setShop_id(str[1]);
                up.setTime_stamp(str[2]);
                return up;
            });

            return paylogs;
        }

        //读取view文件
        private JavaRDD<UserView> parseviewLogFile(String viewpath) {
            JavaRDD<String> lines = jsc.textFile(viewpath);
            //JavaRDD<UserView> viewlogs = lines.map(line -> JSON.parseObject(line, UserView.class));
            JavaRDD<UserView> viewlogs = lines.map(x -> {
                String[] str = x.split(",");
                UserView up = new UserView();
                up.setUser_id(str[0]);
                up.setShop_id(str[1]);
                up.setTime_stamp(str[2]);
                return up;
            });

            return viewlogs;
        }

        final public void runAlkoubei(String shoppath,String paypath,String viewpath) {
            //注册自己编写的类方法
            spark.udf().register("toDayStr", new ToDayFunction(), DataTypes.StringType);

            JavaRDD<ShopInfo> shopinfo = parseShopInfoLogFile(shoppath);
            JavaRDD<UserPay> paylogs = parsePayLogFile(paypath);
            JavaRDD<UserView> viewlogs = parseviewLogFile(viewpath);

            shopinfo.take(10).forEach(x->System.out.println(x.getShop_id()));
            paylogs.take(10).forEach(x->System.out.println(x.getShop_id()));
            viewlogs.take(10).forEach(x->System.out.println(x.getShop_id()));
            //这是调用了class中的过滤方法,在这次实现中,好像没有需要调用,注释后放这里,
            //JavaRDD<Log> validLogs = filterValidLogs(logs);

            process(shopinfo,paylogs,viewlogs);

            spark.close();
        }

    protected JavaPairRDD<ShopInfo,Integer> zipShopInfoInteger(JavaRDD<ShopInfo> shopRDD,JavaPairRDD<String,Integer> shopIdInteger)
    {
        return  shopRDD.mapToPair(s -> new Tuple2<String,ShopInfo>(s.getShop_id(),s))
                .join(shopIdInteger)
                .mapToPair(x -> new Tuple2<ShopInfo,Integer>(x._2._1,x._2._2));
    }

    protected JavaPairRDD<ShopInfo,String> zipShopInfoString(JavaRDD<ShopInfo> shopRDD,JavaRDD<UserPay> payRDD)
    {
        JavaPairRDD<String,String> payDayRDD = payRDD
                .mapToPair(x -> new Tuple2<String,String>(x.getShop_id(),x.getTime_stamp()));

        return  shopRDD.mapToPair(s -> new Tuple2<String,ShopInfo>(s.getShop_id(),s))
                .join(payDayRDD)
                .mapToPair(x -> new Tuple2<ShopInfo,String>(x._2._1,x._2._2));
    }

    protected JavaPairRDD<Double,String> LikeShop(JavaPairRDD<String,Integer> maxpaycount,JavaPairRDD<String,Integer> totalpay,JavaPairRDD<String,Integer> days,JavaRDD<ShopInfo> shops )
    {
        //最受欢迎为(0.7* ( 平均评分 / 5) + 0.3 * (总消费金额/有消费天数/最高消费金额) )
//        return maxpaycount
//                .join(totalpay)
//                .join(days)
//                .mapToPair(x -> new Tuple2<Double,ShopInfo>(
//                        //x._1,
//                        (0.7* (x._1.getPer_pay())/ 5) + 0.3 * (x._2._1._2/x._2._2/x._2._1._1),
//                        x._1
//                        ));
        JavaPairRDD<String,Tuple2<Integer,Integer>> shopAveAndScore = shops
                .mapToPair(x -> new Tuple2<String,Tuple2<Integer,Integer>>(x.getShop_id(),new Tuple2<Integer, Integer>(x.getScore(),x.getPer_pay()) ));

        return maxpaycount
                .join(totalpay)
                .join(days)
                .join(shopAveAndScore)
                //.mapToPair(x -> new Tuple2<Double,String>(((0.7* (x._2._2._1)/ 5) + 0.3 * (x._2._1._1._2* x._2._2._2) / x._2._1._2/( x._2._1._1._1 * x._2._2._2 )) , x._1));
                 .mapToPair(x -> new Tuple2<Double,String>(
                    ((0.7* (x._2._2._1 //score
                    )/ 5) + 0.3 *
                            (x._2._1._1._2  //总消费次数
                                    * x._2._2._2   //Per_pay,
                            ) /
                            x._2._1._2      //days
                            /(
                            x._2._1._1._1       //最高消费次数
                                    *
                                    x._2._2._2
                    )
                    )
                    ,
                    x._1));
    }

    private JavaRDD<ShopInfo> parseShopInfoFile(String path) {
            JavaRDD<String> lines = jsc.textFile(path,10);
            JavaRDD<ShopInfo> ShopInfos = lines.map(line -> JSON.parseObject(line, ShopInfo.class));
            return ShopInfos;
        }

        private JavaRDD<UserPay> parseUserPayFile(String path) {
            JavaRDD<String> lines = jsc.textFile(path);
            JavaRDD<UserPay> paylogs = lines.map(line -> JSON.parseObject(line, UserPay.class));
            return paylogs;
        }
        private JavaRDD<UserView> parseUserViewFile(String path) {
            JavaRDD<String> lines = jsc.textFile(path);
            JavaRDD<UserView> viewlogs = lines.map(line -> JSON.parseObject(line, UserView.class));
            return viewlogs;
        }

        //pay的schema
        protected StructType payschema = new StructType()
                .add("user_id", "string", false)
                .add("shop_id", "string", false)
                .add("time_stamp", "string", false);

        protected Dataset<Row> toPayDataFrame(JavaRDD<UserPay> UserPays) {
            JavaRDD<Row> rowsRDD = UserPays.map(UserPay -> RowFactory.create(
                    UserPay.getUser_id(),
                    UserPay.getShop_id(),
                    UserPay.getTime_stamp()
                    )
            );
            return spark.createDataFrame(rowsRDD, payschema);
        }


        //view的schema
        protected StructType viewschema = new StructType()
                .add("user_id", "string", false)
                .add("shop_id", "string", false)
                .add("time_stamp", "string", false);

        protected Dataset<Row> toViewDataFrame(JavaRDD<UserView> UserViews) {
            JavaRDD<Row> rowsRDD = UserViews.map(UserView -> RowFactory.create(
                    UserView.getUser_id(),
                    UserView.getShop_id(),
                    UserView.getTime_stamp()
                    )
            );
            return spark.createDataFrame(rowsRDD, viewschema);
        }


        //shop的schema
        protected StructType shopschema = new StructType()
                .add("shop_id", "string", false)
                .add("city_name", "string", false)
                .add("location_id", "string", false)
                .add("per_pay", "string", false)
                .add("score", "string", false)
                .add("comment_cnt", "string", false)
                .add("shop_level", "string", false)
                .add("cate_1_name", "string", false)
                .add("cate_2_name", "string", false)
                .add("cate_3_name", "string", false);

        protected Dataset<Row> toShopDataFrame(JavaRDD<ShopInfo> ShopInfos) {
            JavaRDD<Row> rowsRDD = ShopInfos.map(ShopInfo -> RowFactory.create(
                    ShopInfo.getShop_id(),
                    ShopInfo.getCity_name(),
                    ShopInfo.getLocation_id(),
                    ShopInfo.getPer_pay(),
                    ShopInfo.getScore(),
                    ShopInfo.getComment_cnt(),
                    ShopInfo.getShop_level(),
                    ShopInfo.getCate_1_name(),
                    ShopInfo.getCate_2_name(),
                    ShopInfo.getCate_3_name()
                    )
            );
            return spark.createDataFrame(rowsRDD, shopschema);
        }

        protected abstract void process(JavaRDD<ShopInfo> shopInfos,JavaRDD<UserPay> payLogs,JavaRDD<UserView> viewLogs);
}
//内部类实现了一个UDF,将Long类型时间转换成年月日
class ToDayFunction implements UDF1<Long, String> {

    private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

    public static String toDay(long seconds) {
        return format.format(new Date(seconds * 1000));
    }

    public static String toDay(String timeStamp) {
        //if(timeStamp.length() > 10)
          return timeStamp.length() > 0 ? timeStamp:timeStamp.substring(0,10);
    }

    @Override
    public String call(Long s) throws Exception {
        return toDay(s);
    }

    public String call(String s) throws Exception {
        return toDay(s);
    }
}


