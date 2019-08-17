package com.chinamcloud.bigdata.appDataLog;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;


import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * @Classname AppDataLog2Mysql
 * @Description 从kafka读取数据到mysql中
 * @Date 2019/8/14 21:10
 * @Created by yuhousheng
 */
public class AppDataLog2Mysql {

    private static Logger logger = Logger.getLogger(AppDataLog2Mysql.class);

    public static void main(String[] args) throws Exception {

        // 1、准备配置文件
        Properties props = new Properties();
        props.put("bootstrap.servers", "server02:9092,server05:9092,server08:9092");
        props.put("group.id", "app-data-log-cs-g1");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 2、创建KafkaConsumer
        KafkaConsumer<String, String>kafkaConsumer = new KafkaConsumer<String, String>(props);

        // 3、订阅数据，这里的topic可以是多个
        kafkaConsumer.subscribe(Arrays.asList("app-spread-log"));

        /** 将数据插入mysql */
        InputStream inputStream=AppDataLog2Mysql.class.getClassLoader().getResourceAsStream("devDataBase.properties");
        Properties pp = new Properties();
        pp.load(inputStream);

        System.out.println("driverClassName:"+pp.getProperty("driverClassName"));
        System.out.println("url:"+pp.getProperty("url"));
        System.out.println("username:"+pp.getProperty("username"));
        System.out.println("password:"+pp.getProperty("password"));
        System.out.println("initialSize:"+pp.getProperty("initialSize"));
        System.out.println("maxActive:"+pp.getProperty("maxActive"));
        System.out.println("maxWait:"+pp.getProperty("maxWait"));
        System.out.println("maxIdle:"+pp.getProperty("maxIdle"));
        System.out.println("minIdle:"+pp.getProperty("minIdle"));
        //创建连接池，使用配置文件中的参数
        DataSource ds = DruidDataSourceFactory.createDataSource(pp);
        Connection con = ds.getConnection();
        Statement st = con.createStatement();

        HashMap<String, String> valMap = new HashMap<>(5);

        // 4、拉数据
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(10000);
            for (ConsumerRecord<String, String> rec : records) {
                String logStr = rec.value(); // 日志
                System.out.println(logStr);
                /** 从日志中解析指定字段值 */
                // 定义需要提取的字段值
                parse(logStr,valMap);
                String type = valMap.get("type");
                String articleId = valMap.get("articleId");
                String catalogId = valMap.get("catalogId");
                String device = valMap.get("device");
                String eventId = valMap.get("eventId");
                //parse(String logStr,String eventId,String articleId,String catalogId,String device,String type)
                System.out.println("=================================================    type:"+type+","+"eventId:"+eventId+","+"articleId:"+articleId+","+"catalogId:"+catalogId+","+"device:"+device);

                // 时间参数的生成
                FastDateFormat dateFormater = FastDateFormat.getInstance("yyyy-MM-dd HH:00:00", TimeZone.getTimeZone("GMT+8"));
                String timeStr = "'"+dateFormater.format(new Date())+"'"; // 时间：yyyy-MM-dd HH:00:00
                String curDay = timeStr.substring(0,11)+"'"; // 日：yyyy-MM-dd
                System.out.println("curDay: ===========   "+ curDay);
                String hourStr = "'"+timeStr.substring(12,14)+"'"; // 小时字符串：HH

                if (eventId.equals("onPageStartEvent") ||
                        (eventId.equals("onButtonClick") && "-".equals(type))  ) { // 阅读量+1
                    System.out.println("=============================================           阅读量+1 要执行了，不知道成功没");
                    readIncr(st,timeStr,hourStr,curDay,articleId,catalogId,device);
                } else if(!"-".equals(type)) { // 评论/转发/点赞+1
                    switch (type) {
                        case "comment":
                            System.out.println("=============================================           comment 评论+1 要执行了，不知道成功没");
                            commentIncr(st,timeStr,hourStr,curDay,articleId,catalogId,device);
                            System.out.println("comment 评论+1");
                            ;break;


                        case "forward":
                            System.out.println("=============================================           forward 转发+1 要执行了，不知道成功没");
                            forwardIncr(st,timeStr,hourStr,curDay,articleId,catalogId,device);
                            System.out.println("forward 转发+1");
                            ;break;


                        case "praise":
                            System.out.println("=============================================           praise 点赞+1 要执行了，不知道成功没");
                            praiseIncr(st,timeStr,hourStr,curDay,articleId,catalogId,device);
                            System.out.println("praise 点赞+1");
                            ;break;
                        default:break;
                    }
                }
                /*System.out.println("articleId=" + articleId + ","
                        + "catalogId=" + catalogId + ","
                        + "device=" + device + ","
                        + "eventId=" + eventId + ","
                        + "type=" + type + "\n" );*/

                st.executeBatch();
            }
        }

    }


    /**
     * 评论数据+1
     */
    private static void readIncr(Statement st,String timeStr,String hourStr,String curDay,String articleId,String catalogId,String device) throws SQLException {
        String app_article_d = "insert into app_article_d(cal_time,id,catalog_id,uv_cur,pv_cur,share_cur,comment_cur,like_cur,dur_cur,app_uv_cur,js_uv_cur) values("+curDay+","+"'"+articleId+"'"+","+"'"+catalogId+"'"+",0,1,0,0,0,0,0,0) on duplicate key update pv_cur=pv_cur+1 ;" ;
        String app_article_h = "insert into app_article_h(cal_time,hour_str,id,catalog_id,uv_cur,pv_cur,share_cur,comment_cur,like_cur,dur_cur,app_uv_cur,js_uv_cur) values("+timeStr+","+hourStr+","+"'"+articleId+"'"+","+"'"+catalogId+"'"+",0,1,0,0,0,0,0,0) on duplicate key update pv_cur=pv_cur+1 ;" ;
        String app_catalog_d = "insert into app_catalog_d(cal_time,catalog_id,share_cur,js_pv_cur,android_h5_pv_c,android_na_pv_c,android_ot_pv_c,ios_h5_pv_c,ios_na_pv_c,ios_ot_pv_c,pv_cur,comment_cur,like_cur,js_uv_cur,android_h5_uv_c,android_na_uv_c,android_ot_uv_c,ios_h5_uv_c,ios_na_uv_c,ios_ot_uv_c,uv_cur,dur_cur,init_cur,flow_cur,wait_cur,publish_cur,offline_cur,archive_cur,article_total_c,follower_cur,new_follower_cur,un_follower_cur,add_follower_cur) values("+curDay+","+"'"+catalogId+"'"+",0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0) on duplicate key update pv_cur=pv_cur+1 ;" ;
        String app_catalog_h = "insert into app_catalog_h(cal_time,hour_str,catalog_id,share_cur,js_pv_cur,android_h5_pv_c,android_na_pv_c,android_ot_pv_c,ios_h5_pv_c,ios_na_pv_c,ios_ot_pv_c,pv_cur,comment_cur,like_cur,js_uv_cur,android_h5_uv_c,android_na_uv_c,android_ot_uv_c,ios_h5_uv_c,ios_na_uv_c,ios_ot_uv_c,uv_cur,dur_cur,init_cur,flow_cur,wait_cur,publish_cur,offline_cur,archive_cur,article_total_c,follower_cur,new_follower_cur,un_follower_cur,add_follower_cur) values("+timeStr+","+hourStr+","+"'"+catalogId+"'"+",0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0) on duplicate key update pv_cur=pv_cur+1 ;" ;
        String app_phone_brand_d = "insert into app_phone_brand_d(cal_time,catalog_id,phone_brand,js_uv_cur,android_h5_uv_c,android_na_uv_c,android_ot_uv_c,ios_h5_uv_c,ios_na_uv_c,ios_ot_uv_c,uv_cur,js_pv_cur,android_h5_pv_c,android_na_pv_c,android_ot_pv_c,ios_h5_pv_c,ios_na_pv_c,ios_ot_pv_c,pv_cur,follower_cur) values("+curDay+","+"'"+catalogId+"'"+","+"'"+device+"'"+",0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0) on duplicate key update pv_cur=pv_cur+1 ;" ;
        String app_phone_brand_h = "insert into app_phone_brand_h(cal_time,hour_str,catalog_id,phone_brand,js_uv_cur,android_h5_uv_c,android_na_uv_c,android_ot_uv_c,ios_h5_uv_c,ios_na_uv_c,ios_ot_uv_c,uv_cur,js_pv_cur,android_h5_pv_c,android_na_pv_c,android_ot_pv_c,ios_h5_pv_c,ios_na_pv_c,ios_ot_pv_c,pv_cur,follower_cur) values("+timeStr+","+hourStr+","+"'"+catalogId+"'"+","+"'"+device+"'"+",0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0) on duplicate key update pv_cur=pv_cur+1 ;" ;
        String app_user_cnt_d = "insert into app_user_cnt_d(cal_time,catalog_id,prov_name,registered_cur,js_uv_cur,android_h5_uv_c,android_na_uv_c,android_ot_uv_c,ios_h5_uv_c,ios_na_uv_c,ios_ot_uv_c,uv_cur,js_pv_cur,android_h5_pv_c,android_na_pv_c,android_ot_pv_c,ios_h5_pv_c,ios_na_pv_c,ios_ot_pv_c,pv_cur,new_jump_cur,new_back_cur,old_jump_cur,old_back_cur,jump_uv_cur,back_uv_cur,new_uv_cur,old_uv_cur,new_dur_cur,old_dur_cur,dur_cur,activate_cur,follower_cur,new_pv_cur,old_pv_cur,act_uv_cur,act_pv_cur) values("+curDay+","+"'"+catalogId+"'"+","+"'-'"+",0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0) on duplicate key update pv_cur=pv_cur+1 ;" ;
        System.out.println("app_user_cnt_d:  "+app_user_cnt_d);
        String app_user_cnt_h = "insert into app_user_cnt_h(cal_time,hour_str,catalog_id,prov_name,registered_cur,js_uv_cur,android_h5_uv_c,android_na_uv_c,android_ot_uv_c,ios_h5_uv_c,ios_na_uv_c,ios_ot_uv_c,uv_cur,js_pv_cur,android_h5_pv_c,android_na_pv_c,android_ot_pv_c,ios_h5_pv_c,ios_na_pv_c,ios_ot_pv_c,pv_cur,new_jump_cur,new_back_cur,old_jump_cur,old_back_cur,jump_uv_cur,back_uv_cur,new_uv_cur,old_uv_cur,new_dur_cur,old_dur_cur,dur_cur,activate_cur,follower_cur,new_pv_cur,old_pv_cur,act_uv_cur,act_pv_cur) values("+timeStr+","+hourStr+","+"'"+catalogId+"'"+","+"'-'"+",0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0) on duplicate key update pv_cur=pv_cur+1 ;" ;

        st.addBatch(app_article_d) ;
        st.addBatch(app_article_h) ;
        st.addBatch(app_catalog_d) ;
        st.addBatch(app_catalog_h) ;
        st.addBatch(app_phone_brand_d) ;
        st.addBatch(app_phone_brand_h) ;
        st.addBatch(app_user_cnt_d) ;
        st.addBatch(app_user_cnt_h) ;
    }


    /**
     * 转发数据+1
     */
    private static void forwardIncr(Statement st,String timeStr,String hourStr,String curDay,String articleId,String catalogId,String device) throws SQLException {
        String app_article_d_forward = "insert into app_article_d(cal_time,id,catalog_id,uv_cur,pv_cur,share_cur,comment_cur,like_cur,dur_cur,app_uv_cur,js_uv_cur) values("+curDay+","+"'"+articleId+"'"+","+"'"+catalogId+"'"+",0,0,1,0,0,0,0,0) on duplicate key update share_cur=share_cur+1 ;" ;
        String app_article_h_forward = "insert into app_article_h(cal_time,hour_str,id,catalog_id,uv_cur,pv_cur,share_cur,comment_cur,like_cur,dur_cur,app_uv_cur,js_uv_cur) values("+timeStr+","+hourStr+","+"'"+articleId+"'"+","+"'"+catalogId+"'"+",0,0,1,0,0,0,0,0) on duplicate key update share_cur=share_cur+1 ;" ;
        String app_catalog_d_forward = "insert into app_catalog_d(cal_time,catalog_id,share_cur,js_pv_cur,android_h5_pv_c,android_na_pv_c,android_ot_pv_c,ios_h5_pv_c,ios_na_pv_c,ios_ot_pv_c,pv_cur,comment_cur,like_cur,js_uv_cur,android_h5_uv_c,android_na_uv_c,android_ot_uv_c,ios_h5_uv_c,ios_na_uv_c,ios_ot_uv_c,uv_cur,dur_cur,init_cur,flow_cur,wait_cur,publish_cur,offline_cur,archive_cur,article_total_c,follower_cur,new_follower_cur,un_follower_cur,add_follower_cur) values("+curDay+","+"'"+catalogId+"'"+",1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0) on duplicate key update share_cur=share_cur+1 ;" ;
        String app_catalog_h_forward = "insert into app_catalog_h(cal_time,hour_str,catalog_id,share_cur,js_pv_cur,android_h5_pv_c,android_na_pv_c,android_ot_pv_c,ios_h5_pv_c,ios_na_pv_c,ios_ot_pv_c,pv_cur,comment_cur,like_cur,js_uv_cur,android_h5_uv_c,android_na_uv_c,android_ot_uv_c,ios_h5_uv_c,ios_na_uv_c,ios_ot_uv_c,uv_cur,dur_cur,init_cur,flow_cur,wait_cur,publish_cur,offline_cur,archive_cur,article_total_c,follower_cur,new_follower_cur,un_follower_cur,add_follower_cur) values("+timeStr+","+hourStr+","+"'"+catalogId+"'"+",1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0) on duplicate key update share_cur=share_cur+1 ;" ;

        st.addBatch(app_article_d_forward);
        st.addBatch(app_article_h_forward);
        st.addBatch(app_catalog_d_forward);
        st.addBatch(app_catalog_h_forward);
    }


    /**
     * 点赞数据+1
     */
    private static void praiseIncr(Statement st,String timeStr,String hourStr,String curDay,String articleId,String catalogId,String device) throws SQLException {
        String app_article_d_praise = "insert into app_article_d(cal_time,id,catalog_id,uv_cur,pv_cur,share_cur,comment_cur,like_cur,dur_cur,app_uv_cur,js_uv_cur) values("+curDay+","+"'"+articleId+"'"+","+"'"+catalogId+"'"+",0,0,0,0,1,0,0,0) on duplicate key update like_cur=like_cur+1 ;" ;
        String app_article_h_praise = "insert into app_article_h(cal_time,hour_str,id,catalog_id,uv_cur,pv_cur,share_cur,comment_cur,like_cur,dur_cur,app_uv_cur,js_uv_cur) values("+timeStr+","+hourStr+","+"'"+articleId+"'"+","+"'"+catalogId+"'"+",0,0,0,0,1,0,0,0) on duplicate key update like_cur=like_cur+1 ;" ;
        String app_catalog_d_praise = "insert into app_catalog_d(cal_time,catalog_id,share_cur,js_pv_cur,android_h5_pv_c,android_na_pv_c,android_ot_pv_c,ios_h5_pv_c,ios_na_pv_c,ios_ot_pv_c,pv_cur,comment_cur,like_cur,js_uv_cur,android_h5_uv_c,android_na_uv_c,android_ot_uv_c,ios_h5_uv_c,ios_na_uv_c,ios_ot_uv_c,uv_cur,dur_cur,init_cur,flow_cur,wait_cur,publish_cur,offline_cur,archive_cur,article_total_c,follower_cur,new_follower_cur,un_follower_cur,add_follower_cur) values("+curDay+","+"'"+catalogId+"'"+",0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0) on duplicate key update like_cur=like_cur+1 ;" ;
        String app_catalog_h_praise = "insert into app_catalog_h(cal_time,hour_str,catalog_id,share_cur,js_pv_cur,android_h5_pv_c,android_na_pv_c,android_ot_pv_c,ios_h5_pv_c,ios_na_pv_c,ios_ot_pv_c,pv_cur,comment_cur,like_cur,js_uv_cur,android_h5_uv_c,android_na_uv_c,android_ot_uv_c,ios_h5_uv_c,ios_na_uv_c,ios_ot_uv_c,uv_cur,dur_cur,init_cur,flow_cur,wait_cur,publish_cur,offline_cur,archive_cur,article_total_c,follower_cur,new_follower_cur,un_follower_cur,add_follower_cur) values("+timeStr+","+hourStr+","+"'"+catalogId+"'"+",0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0) on duplicate key update like_cur=like_cur+1 ;" ;

        st.addBatch(app_article_d_praise);
        st.addBatch(app_article_h_praise);
        st.addBatch(app_catalog_d_praise);
        st.addBatch(app_catalog_h_praise);
    }


    /**
     * 阅读量+1
     */
    private static void commentIncr(Statement st,String timeStr,String hourStr,String curDay,String articleId,String catalogId,String device) throws SQLException {
        String app_article_d_comment = "insert into app_article_d(cal_time,id,catalog_id,uv_cur,pv_cur,share_cur,comment_cur,like_cur,dur_cur,app_uv_cur,js_uv_cur) values("+curDay+","+"'"+articleId+"'"+","+"'"+catalogId+"'"+",0,0,0,1,0,0,0,0) on duplicate key update comment_cur=comment_cur+1 ;" ;
        String app_article_h_comment = "insert into app_article_h(cal_time,hour_str,id,catalog_id,uv_cur,pv_cur,share_cur,comment_cur,like_cur,dur_cur,app_uv_cur,js_uv_cur) values("+timeStr+","+hourStr+","+"'"+articleId+"'"+","+"'"+catalogId+"'"+",0,0,0,1,0,0,0,0) on duplicate key update comment_cur=comment_cur+1 ;" ;
        String app_catalog_d_comment = "insert into app_catalog_d(cal_time,catalog_id,share_cur,js_pv_cur,android_h5_pv_c,android_na_pv_c,android_ot_pv_c,ios_h5_pv_c,ios_na_pv_c,ios_ot_pv_c,pv_cur,comment_cur,like_cur,js_uv_cur,android_h5_uv_c,android_na_uv_c,android_ot_uv_c,ios_h5_uv_c,ios_na_uv_c,ios_ot_uv_c,uv_cur,dur_cur,init_cur,flow_cur,wait_cur,publish_cur,offline_cur,archive_cur,article_total_c,follower_cur,new_follower_cur,un_follower_cur,add_follower_cur) values("+curDay+","+"'"+catalogId+"'"+",0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0) on duplicate key update comment_cur=comment_cur+1 ;" ;
        String app_catalog_h_comment = "insert into app_catalog_h(cal_time,hour_str,catalog_id,share_cur,js_pv_cur,android_h5_pv_c,android_na_pv_c,android_ot_pv_c,ios_h5_pv_c,ios_na_pv_c,ios_ot_pv_c,pv_cur,comment_cur,like_cur,js_uv_cur,android_h5_uv_c,android_na_uv_c,android_ot_uv_c,ios_h5_uv_c,ios_na_uv_c,ios_ot_uv_c,uv_cur,dur_cur,init_cur,flow_cur,wait_cur,publish_cur,offline_cur,archive_cur,article_total_c,follower_cur,new_follower_cur,un_follower_cur,add_follower_cur) values("+timeStr+","+hourStr+","+"'"+catalogId+"'"+",0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0) on duplicate key update comment_cur=comment_cur+1 ;" ;

        st.addBatch(app_article_d_comment);
        st.addBatch(app_article_h_comment);
        st.addBatch(app_catalog_d_comment);
        st.addBatch(app_catalog_h_comment);
    }


    /**
     * 解析字段值
     */
    private static void parse(String logStr, Map<String, String> valMap) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Map map = mapper.readValue(logStr, Map.class); // logMap
        Object record = map.get("record");
        try {
            Map recordMap = mapper.readValue(record.toString(), Map.class); // recordMap

            Map bodyMap = (Map<String, Object>) recordMap.get("body");  // bodyMap

            String type = "";
            String articleId = "";
            String catalogId = "";
            String device = "";
            String eventId = "";

            // 定义需要提取的字段值
            eventId = null == bodyMap.get("eventId") ? "-" : bodyMap.get("eventId").toString();

            Map dataMap = (Map<String, Object>) bodyMap.get("data");
            if (null == dataMap) { // 没有dataMap
                articleId = "-";
                catalogId = "-";
                device = "-";
                type = "-";
            } else { // 都可以从dataMap取值的字段
                articleId = null == dataMap.get("articleId") ? "-" : dataMap.get("articleId").toString();
                catalogId = null == dataMap.get("catalogId") ? "-" : dataMap.get("catalogId").toString();
                device = null == dataMap.get("device") ? "-" : dataMap.get("device").toString();
            }
            Object vData = dataMap.get("vData");
            if (null == vData) { //没有vData
                type = "-";
            } else {
                try {
                    Map vDataMap = mapper.readValue(vData.toString(), Map.class);
                    type = null == vDataMap.get("type") ? "-" : vDataMap.get("type").toString();
                    articleId = null == vDataMap.get("articleId") ? "-" : vDataMap.get("articleId").toString(); // 有vData的，articleId,catalogId在这层里面
                    catalogId = null == vDataMap.get("catalogId") ? "-" : vDataMap.get("catalogId").toString();
                } catch (JsonParseException e) { // vData是一个map对象
                    Map vDataMap2 = (Map<String, Object>) vData;
                    type = null == vDataMap2.get("type") ? "-" : vDataMap2.get("type").toString();
                    articleId = null == vDataMap2.get("articleId") ? "-" : vDataMap2.get("articleId").toString();
                    catalogId = null == vDataMap2.get("catalogId") ? "-" : vDataMap2.get("catalogId").toString();
                }
            }


            valMap.put("type", type);
            valMap.put("articleId", articleId);
            valMap.put("catalogId", catalogId);
            valMap.put("device", device);
            valMap.put("eventId", eventId);

        } catch (JsonParseException e) {
            System.out.println(" ##Parsing log## "+logStr);
            System.out.println(" ##erro detail## "+e.getMessage());
        }
    }

}
