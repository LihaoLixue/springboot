package com.example.demo.until.ddmessage;


import com.alibaba.fastjson.JSONObject;

import java.io.*;
import java.net.*;
import java.text.SimpleDateFormat;
import java.util.Properties;

import static com.example.demo.until.ddmessage.AssignMessage.getAccessToken;

/**
 * @author LH
 * @description:
 * @date 2021-12-06 16:05
 */
public class test {
    public test() throws IOException {
    }

    public static void main(String[] args) throws Exception {


//        String str="10|105000000357|彭永斌,10|105000003456|忻吟文,10|105000014820|李宇霏,10|105087019072|朱新瑶,10|105050001165|张华庆,1|105000002075|章元根,10|105087014841|侯砚文,10|105000014623|曹斌,1|105000014683|嵇雪锋";
//        String[] split = str.split(",");
//        String message_10="";
//        String message_1="";
//        String message_0="";
//        for(int i=0;i<split.length;i++){
//            if (split[i].contains("10|")){
//                String[] split1 = split[i].split("\\|");
//                message_10=message_10+"  \n  "+split1[2]+"(客户号："+split1[1]+")、";
//            }else if(split[i].contains("1|")){
//                String[] split1 = split[i].split("\\|");
//                message_1=message_1+"  \n  "+split1[2]+"(客户号："+split1[1]+")、";
//            }else if(split[i].contains("0|")){
//            String[] split1 = split[i].split("\\|");
//                message_0=message_0+"  \n  "+split1[2]+"(客户号："+split1[1]+")";
//            }
//        }
//        String message="";
//        if (message_10.length()>0&&message_0.length()>0) {
//            String substring = message_0.substring(0, message_0.length() - 1);
//            String substring10 = message_10.substring(0, message_10.length() - 1);
//            message = "你部" +  substring+ "有融资融券合约将于今日到期;" + message_1 + substring10 + "  \n  有融资融券合约将于10个交易日到期，请前往风险监控平台——预警中心查看处理。";
//        }else if(message_10.length()>0&&message_0.length()==0){
//            String substring10 = message_10.substring(0, message_10.length() - 1);
//            message = "你部" + message_1 + substring10+ "  \n  有融资融券合约将于10个交易日到期，请前往风险监控平台——预警中心查看处理。";
//        }else if(message_10.length()==0&&message_0.length()==0&&message_1.length()>0){
//            String substring1 = message_1.substring(0, message_1.length() - 1);
//            message = "你部" + message_1.substring(0, message_1.length() - 1) + "  \n  有融资融券合约将于今日到期,请前往风险监控平台——预警中心查看处理。";
//        }
//        System.out.println(message);
//        String accessToken = getAccessToken();
//        AssignMessage.assignXX_only_dd(accessToken,"002968", message);

//        JSONObject jsonObject = JSONObject.parseObject(str);
//        System.out.println(jsonObject);
        SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String dateStr = dateformat.format(System.currentTimeMillis());
//        assignXX_only_dx("15952028198", dateStr + " 您有一条维持担保比例告警，请您及时到预警中心查看处理");
//        String dateStr = dateformat.format(System.currentTimeMillis());
//        System.out.println(dateStr);
//        assignXX_only_dd_1("002968","# 【南京证券-信用风险监控】\n 测试内容");
//        String accessToken = getAccessToken();
////        System.out.println(accessToken);
//        long l = System.currentTimeMillis();
//        SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//
//        String dateStr = dateformat.format(System.currentTimeMillis());
//        System.out.println(dateStr);

    }


    public static void assignXX_only_dd_1(String user_id,String cotent) throws Exception {
        String accessToken = getAccessToken();
        System.out.println(accessToken);
        Properties prop = System.getProperties();
        // 设置http访问要使用的代理服务器的地址
        prop.setProperty("http.proxyHost", "10.254.255.55");
        prop.setProperty("http.proxyPort", "3128");
        // 对https也开启代理
        System.setProperty("https.proxyHost", "10.254.255.55");
        System.setProperty("https.proxyPort", "3128");
        //设置请求访问的地址
        URL url = new URL("https://oapi.dingtalk.com/topapi/message/corpconversation/asyncsend_v2?access_token="+accessToken);
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
//        jsonParam1.put("single_url", "http://www.njzq.com.cn/njzq/index.jsp");
//        jsonParam1.put("markdown", cotent);
//        jsonParam3.put("msgtype", "action_card");
//        jsonParam3.put("action_card", jsonParam1);

        jsonParam1.put("title","【南京证券-信用风险监控】");
        jsonParam1.put("text",cotent);
        jsonParam3.put("markdown",jsonParam1);
        jsonParam3.put("msgtype","markdown");

//        jsonParam1.put("content",cotent);
//        jsonParam3.put("text",jsonParam1);
//        jsonParam3.put("msgtype","text");



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
