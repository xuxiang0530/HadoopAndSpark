package com.xuxiang.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

public class UserPay implements Serializable {
    @JsonProperty("user_id") private String user_id;
    @JsonProperty("shop_id") private String shop_id;
    @JsonProperty("time_stamp") private String time_stamp;

    public String getUser_id() {
        return user_id;
    }


    private static String isNull(String value)
    {
        if(value==null||value.isEmpty())
            return "";
        else
            return  value;
    }

    public void setUser_id(String user_id) {
        this.user_id = isNull(user_id);
    }

    public String getShop_id() {
        return shop_id;
    }

    public void setShop_id(String shop_id) {
        this.shop_id = isNull(shop_id);
    }

    public String getTime_stamp() {
        return time_stamp.trim().length() > 10? time_stamp.trim().substring(0,10):time_stamp;
    }

    public void setTime_stamp(String time_stamp) {
        this.time_stamp = isNull(time_stamp);
    }
}
