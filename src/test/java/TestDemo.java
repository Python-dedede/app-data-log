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


        InputStream inputStream = AppDataLog2Mysql.class.getClassLoader().getResourceAsStream("devDataBase.properties");
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
        con.close();


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
