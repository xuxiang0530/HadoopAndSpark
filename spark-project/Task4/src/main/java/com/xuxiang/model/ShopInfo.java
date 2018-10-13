package com.xuxiang.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class ShopInfo implements Serializable {
    @JsonProperty("shop_id") private String shop_id;
    @JsonProperty("city_name") private String city_name;
    @JsonProperty("location_id") private Integer location_id;
    @JsonProperty("per_pay") private Integer per_pay;
    @JsonProperty("score") private Integer score;
    @JsonProperty("comment_cnt") private Integer comment_cnt;
    @JsonProperty("shop_level") private Integer shop_level;
    @JsonProperty("cate_1_name") private String cate_1_name;
    @JsonProperty("cate_2_name") private String cate_2_name;
    @JsonProperty("cate_3_name") private String cate_3_name;


    private static String isNull(String value)
    {
        if(value==null||value.isEmpty())
            return "";
        else
            return  value;
    }

    private static Integer toInt(String value) {
        if (value == null || value.isEmpty())
            return 0;
        else
            return Integer.parseInt(value);
    }
        public boolean isHotPot() {
        return cate_1_name.contains("火锅") || cate_2_name.contains("火锅") ||cate_3_name.contains("火锅") ;
    }
    public String getShop_id() {
        return shop_id;
    }
    @JsonProperty("shop_id")
    public void setShop_id(String shop_id) {
        this.shop_id = isNull(shop_id);
    }

    public String getCity_name() {
        return city_name;
    }

    public void setCity_name(String city_name) {
        this.city_name = isNull(city_name);
    }

    public Integer getLocation_id() {
        return location_id;
    }

    public void setLocation_id(String location_id) {
        this.location_id = toInt(location_id);
    }
    public void setLocation_id(Integer location_id) {
        this.location_id =  location_id;
    }

    public Integer getPer_pay() {
        return per_pay;
    }

    public void setPer_pay(Integer per_pay) {
        this.per_pay = per_pay;
    }

    public void setPer_pay(String per_pay) {
        this.per_pay = toInt(per_pay);
    }
    public Integer getScore() {
        return score;
    }

    public void setScore(Integer score) {
        this.score = score;
    }

    public void setScore(String score) {
        this.score = toInt(score);
    }

    public Integer getComment_cnt() {
        return comment_cnt;
    }

    public void setComment_cnt(Integer comment_cnt) {
        this.comment_cnt = comment_cnt;
    }
    public void setComment_cnt(String comment_cnt) {
        this.comment_cnt = toInt(comment_cnt);
    }

    public Integer getShop_level() {
        return shop_level;
    }

    public void setShop_level(Integer shop_level) {
        this.shop_level = shop_level;
    }
    public void setShop_level(String shop_level) {
        this.shop_level = toInt(shop_level);
    }

    public String getCate_1_name() {
        return cate_1_name;
    }

    public void setCate_1_name(String cate_1_name) {
        this.cate_1_name = isNull(cate_1_name);
    }

    public String getCate_2_name() {
        return cate_2_name;
    }

    public void setCate_2_name(String cate_2_name) {
        this.cate_2_name = isNull(cate_2_name);
    }

    public String getCate_3_name() {
        return cate_3_name;
    }

    public void setCate_3_name(String cate_3_name) {
        this.cate_3_name = isNull(cate_3_name);
    }
}
