import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.chinamcloud.bigdata.appDataLog.AppDataLog2Mysql;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;


import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @Classname TestDemo
 * @Description TODO
 * @Date 2019/8/15 10:51
 * @Created by yuhousheng
 */
public class TestDemo {

    private static final Logger logger = Logger.getLogger(TestDemo.class);

    public static void main(String[] args) throws Exception {
        /*FastDateFormat dateFormater = FastDateFormat.getInstance("yyyy-MM-dd HH:00:00", TimeZone.getTimeZone("GMT+8"));
        String timeStr = "'"+dateFormater.format(new Date())+"'"; // 时间：yyyy-MM-dd HH:00:00
        String curDay = timeStr.substring(0,11)+"'"; // 日：yyyy-MM-dd
        String hourStr = "'"+timeStr.substring(12,14)+"'"; // 小时字符串：HH

        System.out.println("timeStr:"+timeStr);
        System.out.println("curDay:"+curDay);
        System.out.println("hourStr:"+hourStr);
*/


        /*BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\31883\\Downloads\\data.log"));
        String line = "";
        int count = 0;

        while ((line = br.readLine()) != null) {
            System.out.println("line:  " + line);
            ObjectMapper mapper = new ObjectMapper();
            Map map = mapper.readValue(line, Map.class);
            Object record = map.get("record");
            Map recordMap = mapper.readValue(record.toString(), Map.class); // recordMap
            Map bodyMap = (Map<String, Object>) recordMap.get("body");  // bodyMap

            // 定义需要提取的字段值
            String type = "";
            String articleId = "";
            String catalogId = "";
            String device = "";
            String eventId = null==bodyMap.get("eventId")?"-":bodyMap.get("eventId").toString();

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
                    type = vDataMap.get("type").toString();
                } catch (JsonParseException e) { // vData是一个map对象
                    Map vDataMap2 = (Map<String, Object>) vData;
                    type = null == vDataMap2.get("type") ? "-" : vDataMap2.get("type").toString();
                }
            }

            System.out.println(eventId+","+articleId+","+catalogId+","+device+","+type);
        }*/


        /*InputStream inputStream = AppDataLog2Mysql.class.getClassLoader().getResourceAsStream("devDataBase.properties");
        Properties pp = new Properties();
        pp.load(inputStream);
        //创建连接池，使用配置文件中的参数
        DataSource ds = DruidDataSourceFactory.createDataSource(pp);
        Connection con = ds.getConnection();
        Statement st = con.createStatement();

        String sql = "select id,pv,name from szmg_data_hive_analysis.tmp_yuhs_1 ;";
        logger.info("##query sql##"+sql);
        ResultSet rs = st.executeQuery(sql);

        while (rs.next()) {
            System.out.println(
                    rs.getInt("id")
                            + "," + rs.getInt("pv")
                            + "," + rs.getString("name")
            );
        }

        st.close();
        con.close();*/

        String logStr = "{\"ip\":\"223.104.186.174\",\"record\":\"POST /collector HTTP/1.1\\r\\nHost: 47.94.90.194:8089\\r\\nContent-Type: application/json;encoding=utf-8\\r\\nConnection: keep-alive\\r\\nAccept: */*\\r\\nUser-Agent: %E5%A4%A9%E4%B8%8B%E6%B3%89%E5%9F%8E/1 CFNetwork/758.2.8 Darwin/15.0.0\\r\\nContent-Length: 539\\r\\nAccept-Language: zh-cn\\r\\nAccept-Encoding: gzip, deflate\\r\\n\\r\\n{\\\"version\\\":\\\"\\\",\\\"secret\\\":\\\"\\\",\\\"body\\\":{\\\"longitude\\\":\\\"117.02\\\",\\\"deviceId\\\":\\\"0f39f67aa78ba52af4ca43f63bcca35b36a1d5a3\\\",\\\"latitude\\\":\\\"36.66\\\",\\\"tenantId\\\":\\\"jntv\\\",\\\"userId\\\":\\\"89003\\\",\\\"salt\\\":\\\"27056B13-5278-4185-96BE-9CCF814DB16F\\\",\\\"platform\\\":\\\"ios\\\",\\\"eventId\\\":\\\"onAp\",\"receiveTs\":\"1565999567079\"}";
        HashMap<String, String> hm = new HashMap<String, String>(5);
        parse(logStr, hm);
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
            logger.error(e.getMessage());
        }
    }


    /**
     * 解析日志，取eventId
     */
    public static String getEventId(String logStr) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Map bodyMap = getBodyMap(logStr);
        String eventId = bodyMap.get("eventId").toString();
        return eventId;
    }

    /**
     * 解析日志，取type【comment/praise/forward】
     */
//    articleId log - record - body - data
//    catalogId log - record - body - data
//    device    log - record - body - data
//    eventId   log - record - body
//    type      log - record - body - data - vData
    public static String getType(String logStr) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Map map = mapper.readValue(logStr, Map.class);
        Object record = map.get("record");
        Map recordMap = mapper.readValue(record.toString(), Map.class); // recordMap
        Map bodyMap = (Map<String, Object>) recordMap.get("body");  // bodyMap

        // 定义需要提取的字段值
        String type = "";
        String articleId = "";
        String catalogId = "";
        String device = "";
        String eventId = null == bodyMap.get("eventId") ? "-" : bodyMap.get("eventId").toString();

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
                type = vDataMap.get("type").toString();
            } catch (JsonParseException e) { // vData是一个map对象
                Map vDataMap2 = (Map<String, Object>) vData;
                type = null == vDataMap2.get("type") ? "-" : vDataMap2.get("type").toString();
            }
        }
        return "articleId=" + articleId + ","
                + "catalogId=" + catalogId + ","
                + "device=" + device + ","
                + "eventId=" + eventId + ","
                + "type=" + type + "\n";
    }

    public static Map getBodyMap(String logStr) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Map map = mapper.readValue(logStr, Map.class);
        Object record = map.get("record");
        Map recordMap = mapper.readValue(record.toString(), Map.class);
        Map body = (Map<String, Object>) recordMap.get("body");
        return body;
    }


/*    public static Map getBodyMap(String logStr) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Map map = mapper.readValue(logStr, Map.class);
        Object record = map.get("record");
        Map recordMap = mapper.readValue(record.toString(), Map.class);
        Map body = (Map<String,Object>)recordMap.get("body");
        System.out.println("body: ==    "+body);
        System.out.println("body.get(\"eventId\"): "+body.get("eventId"));
        System.out.println("body.get(\"data\"): "+body.get("data"));
        Map dataMap = (Map<String,Object>)body.get("data");
        Object vData = dataMap.get("vData");
        if(null!=vData) {
            System.out.println("********************************************dataMap.get(\"vData\"): " + vData.toString());
            try {
                Map vDataMap = mapper.readValue(vData.toString(), Map.class);
                System.out.println("-----------------------------vDataMap.get(\"type\"):-----   "+vDataMap.get("type"));
            } catch (JsonParseException e) {
                Map vDataMap2 = (Map<String,Object>)vData;
                System.out.println("222222222222222222222222222222222222          vDataMap2.get(\"type\"):-----   "+vDataMap2.get("type"));
            }
        }
        return body;
    }*/
}
