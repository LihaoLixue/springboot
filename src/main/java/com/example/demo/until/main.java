package com.example.demo.until;

import com.alibaba.fastjson.JSONObject;
import com.example.demo.until.mysql.MySQLUtil;
import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import static com.example.demo.until.ddmessage.AssignMessage.getAccessToken;


/**
 * @author LH
 * @description:
 * @date 2021-09-17 10:52
 */
public class main {
    static ComboPooledDataSource dataSource;
    private static Connection connection;
    public main() {
        dataSource = MySQLUtil.getConnection_1();
    }

    public static void main(String[] args) {
        main m = new main();
        String accessToken = null;
        try {
            accessToken = getAccessToken();
        } catch (Exception e) {
            e.printStackTrace();
        }
        String yybmc = "南京大东亭营业部";
        String khxx = "10|000013001104|赵良栋,10|000013021502|何仁德,10|000013026250|马秀花,10|000013011735|韩晓兵,10|000013000476|耿银苹,0|000013023555|梁煜华,10|000013012773|宗晓东,10|000013009521|冯金元,0|000013009345|梁小伟,10|000013007457|史文喜,10|000013006128|黄中华,10|000013013396|梁志华,10|000013019761|祁国燕,10|000013029137|闫志,10|000013013269|张瑞梅,10|000013001942|朱建国,10|000013005809|詹志敏,10|000013023568|欧阳国乾,10|000013028827|柴立红,10|000013018832|白冰,10|000013000380|冶强,10|000013021553|陈健,10|000013029736|王海燕,10|000013009345|梁小伟,10|000013010191|齐擎,10|000013014305|胡伟,10|000013009119|申振丽";
        String ygh = "002968";
        SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss ");
        String dateStr = dateformat.format(System.currentTimeMillis());
        String message = alertHtdqMessage(khxx);
        try {
//            assignXX_only_dd(accessToken, ygh, yybmc + "," + message + dateStr);
            write_ff(khxx,ygh);

            if(!connection.isClosed()){
                connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void write_ff(String khxx, String gyh) {
        String[] split = khxx.split(",");
        for (String mess : split) {
            String dqlx_mess = null;
            String khh = null;
            String khxm = null;
            try {
                String[] split_mess = mess.split("\\|");
                dqlx_mess = split_mess[0];
                khh = split_mess[1];
                khxm = split_mess[2];
            } catch (ArrayIndexOutOfBoundsException e) {
                dqlx_mess = "00";
                khh = "00";
                khxm = "00";
                System.out.println("数据格式错误，请检查！" + e.getMessage());
                String accessToken = null;
                try {
                    accessToken = getAccessToken();
                    assignXX_only_dd(accessToken, "002968", "数据格式错误，请检查。" + mess);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
            String sql = "insert into margin_txy_mess_record(khh,khxm,gjlx,rece,rq,time) values (?,?,?,?,?,?)";
            PreparedStatement pst = null;
            try {
                connection = dataSource.getConnection();
                //用来执行SQL语句查询，对sql语句进行预编译处理
                SimpleDateFormat sdf = new SimpleDateFormat();// 格式化时间
                sdf.applyPattern("yyyyMMdd HH:mm:ss");// a为am/pm的标记
                Date date = new Date();// 获取当前时间
                String format = sdf.format(date);
                String[] s = format.split(" ");
                String rq = s[0];
                String time = s[1];
                pst = connection.prepareStatement(sql);
                pst.setString(1, khh);
                pst.setString(2, khxm);
                pst.setString(3, dqlx_mess);
                pst.setString(4, gyh);
                pst.setString(5, rq);
                pst.setString(6, time);
                pst.executeUpdate();
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
            try {
                if (!pst.isClosed()){
                    pst.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    public static String alertHtdqMessage(String khxx) {
        String[] split = khxx.split(",");
        String message_10 = "";
        String message_1 = "";
        String message_0 = "";
        for (String mess : split) {
            String dqlx_mess = null;
            String khh = null;
            String khxm = null;
            try {
                String[] split_mess = mess.split("\\|");
                dqlx_mess = split_mess[0];
                khh = split_mess[1];
                khxm = split_mess[2];
            } catch (ArrayIndexOutOfBoundsException e) {
                dqlx_mess = "00";
                khh = "00";
                khxm = "00";
                System.out.println("数据格式错误，请检查！" + e.getMessage());
                String accessToken = null;
                try {
                    accessToken = getAccessToken();
                    assignXX_only_dd(accessToken, "002968", "数据格式错误，请检查。" + mess);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
            switch (dqlx_mess) {
                case "10":
                    message_10 = message_10 + "  \n  " + khxm + "(客户号：" + khh + ")、";
                    break;
                case "1":
                    message_1 = message_1 + "  \n  " + khxm + "(客户号：" + khh + ")、";
                    break;
                case "0":
                    message_0 = message_0 + "  \n  " + khxm + "(客户号：" + khh + ")、";
                    break;
            }
        }
        String message = "";
        if (message_10.length() > 0 && message_0.length() > 0) {
            String substring = message_0.substring(0, message_0.length() - 1);
            String substring10 = message_10.substring(0, message_10.length() - 1);
            message = "你部" + substring + "有融资融券合约将于今日到期;" + message_1 + substring10 + "  \n  有融资融券合约将于10个交易日到期，请前往风险监控平台——预警中心查看处理。";
        } else if (message_10.length() > 0 && message_0.length() == 0) {
            String substring10 = message_10.substring(0, message_10.length() - 1);
            message = "你部" + message_1 + substring10 + "  \n  有融资融券合约将于10个交易日到期，请前往风险监控平台——预警中心查看处理。";
        } else if (message_10.length() == 0 && message_0.length() == 0 && message_1.length() > 0) {
            String substring1 = message_1.substring(0, message_1.length() - 1);
            message = "你部" + message_1.substring(0, message_1.length() - 1) + "  \n  有融资融券合约将于今日到期,请前往风险监控平台——预警中心查看处理。";
        }
        return message;
    }

    public void writeMysql() {
        String Url = "jdbc:mysql://192.168.31.162:3306/socks?useUnicode=true&characterEncoding=UTF-8";
        String JDBCDriver = "com.mysql.jdbc.Driver";
        long l = System.currentTimeMillis();
        String sql = "insert into write_message(khh,khxm,gjlx,rece,rq,time) values (?,?,?,?,?)";
        Connection con = null;
        try {
            Class.forName(JDBCDriver);
            con = DriverManager.getConnection(Url, "root", "123456");
            //用来执行SQL语句查询，对sql语句进行预编译处理
            PreparedStatement pst = con.prepareStatement(sql);
//            pst.setString(1, response.getAccessToken());
            pst.setString(2, String.valueOf(l));
            pst.executeUpdate();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static void assignXX_only_dd(String accessToken, String user_id, String cotent) throws Exception {
//        String accessToken = getAccessToken();
        Properties prop = System.getProperties();
        // 设置http访问要使用的代理服务器的地址
        prop.setProperty("http.proxyHost", "10.254.255.55");
        prop.setProperty("http.proxyPort", "3128");
        // 对https也开启代理
        System.setProperty("https.proxyHost", "10.254.255.55");
        System.setProperty("https.proxyPort", "3128");
        //设置请求访问的地址
        URL url = new URL("https://oapi.dingtalk.com/topapi/message/corpconversation/asyncsend_v2?access_token=" + accessToken);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.setDoInput(true);
        conn.setUseCaches(false);
        conn.setRequestProperty("Connection", "Keep-Alive");
        conn.setRequestProperty("Charset", "UTF-8");
        conn.setConnectTimeout(10000);
        conn.setReadTimeout(10000);
        // 设置文件类型:
        conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
        // 设置接收类型否则返回415错误
        //conn.setRequestProperty("accept","*/*")此处为暴力方法设置接受所有类型，以此来防范返回415;
        conn.setRequestProperty("accept", "application/json");
        conn.connect();

/**
 *
 *    msg.setMsgtype("markdown");
 * //            msg.setMarkdown(new OapiMessageCorpconversationAsyncsendV2Request.Markdown());
 * //            msg.getMarkdown().setText(message);
 * //            msg.getMarkdown().setTitle("【南京证券-信用风险监控】");
 * */
        JSONObject jsonParam = new JSONObject();
        JSONObject jsonParam1 = new JSONObject();
        JSONObject jsonParam3 = new JSONObject();

//        jsonParam1.put("title", "【南京证券-信用风险监控】");
//        jsonParam1.put("single_title", "查看详情");
//        jsonParam1.put("single_url", "");
//        jsonParam1.put("markdown", cotent);
//        jsonParam3.put("msgtype", "action_card");
//        jsonParam3.put("action_card", jsonParam1);
        jsonParam1.put("title", "【南京证券-信用风险监控】");
        jsonParam1.put("text", cotent);
        jsonParam3.put("markdown", jsonParam1);
        jsonParam3.put("msgtype", "markdown");


        jsonParam.put("agent_id", 1390330698L);
        jsonParam.put("userid_list", user_id);
        jsonParam.put("msg", jsonParam3);

        String jsonStr = JSONObject.toJSONString(jsonParam);
        System.out.println(jsonStr);
        DataOutputStream printout;
        DataInputStream input;

        printout = new DataOutputStream(conn.getOutputStream());
//        printout.writeBytes(URLEncoder.encode(str,"UTF-8"));
        printout.write(jsonStr.getBytes());
        printout.flush();
        printout.close();

        int HttpResult = conn.getResponseCode();
        System.out.println(HttpResult);

        if (HttpResult == HttpURLConnection.HTTP_OK) {
            BufferedReader br = new BufferedReader(new InputStreamReader(
                    conn.getInputStream(), "utf-8"));
            String line = null;
            StringBuffer sb = new StringBuffer("");

            while ((line = br.readLine()) != null) {
                sb.append(line + "\n");
                System.out.println(sb);
            }
//            System.out.println(sb);
            br.close();

            //System.out.println(""+sb.toString());

        } else {
            System.out.println(conn.getResponseMessage());
        }
//        InputStream inputStream = conn.getInputStream();
//        //读取结果
//        byte[] bytes = new byte[1024];
//        while (inputStream.read(bytes) >= 0) {
//            System.out.println(new String(bytes));
//        }
    }

}
