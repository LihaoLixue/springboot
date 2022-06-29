package com.example.demo.until.ddmessage;

import com.alibaba.fastjson.JSONObject;
import com.dingtalk.api.DefaultDingTalkClient;
import com.dingtalk.api.DingTalkClient;
import com.dingtalk.api.request.OapiMessageCorpconversationAsyncsendV2Request;
import com.dingtalk.api.request.OapiV2UserGetbymobileRequest;
import com.dingtalk.api.response.OapiMessageCorpconversationAsyncsendV2Response;
import com.dingtalk.api.response.OapiV2UserGetbymobileResponse;
import com.taobao.api.ApiException;
import org.apache.log4j.Logger;
import sun.misc.BASE64Encoder;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;


/**
 * @author LH
 * @description:
 * @date 2021-09-03 14:21
 */
public class AssignMessage {
    static Logger logger = Logger.getLogger(AssignMessage.class);


    /**
     * 方法名称：assignXX
     * 功    能：多人一次性发送
     * 参    数：user_id list，message
     * 返 回 值：null
     */
    public static void assignXX(List<String> sjhm, String message) {
        System.out.println("进入发送短信逻辑");
        String sjh = "null";
        for (int i = 0; i < sjhm.size(); i++) {
            if (i == sjhm.size() - 1) {
                if (sjh.equals("null")) {
                    sjh = sjhm.get(i);
                } else {
                    sjh = sjh + sjhm.get(i);
                }

            } else {
                sjh = sjhm.get(i) + ",";
            }
        }
        // 接口地址
        String ulr = "https://95386.njzq.cn:9000/sms/send.do?";
        // 客户ID，必填
        String userid = "400039";
        // 时间戳，必填
        String timespan = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());// 当前时间转化为yyyymmddhhmmss;
        // 原始密码，必填
        String password = "lrjk892022";
        // MD5加密后密码
        String pwd = getMD5(password + timespan);//原始密码+时间戳做MD5加密，32位大写格式
        // 手机号，必填
        String mobile = sjh;
        // 内容，必填
        String content = message;
        // 扩展，选填，可为空，请先询问是否有扩展权限
        String ext = "";
        // 定时时间，选填，可为空
        String attime = "";
        // 选填，如果不填默认为GBK，可以选填GBK或者UTF8/UTF-8
        String msgfmt = "gbk";
        StringBuilder sendData = new StringBuilder();
        BASE64Encoder encoder = new BASE64Encoder();
        try {
            String content_base64 = encoder.encode(content.getBytes(msgfmt));// 做base64加密操作,编码方式使用msgfmt
            sendData.append("userid=").append(userid)
                    .append("&pwd=").append(pwd)
                    .append("&timespan=").append(timespan)
                    .append("&mobile=").append(mobile)
                    .append("&content=").append(content_base64)
                    .append("&ext=").append(ext)
                    .append("&attime=").append(attime);

            // 发送短信
            String result = httpPost(ulr, sendData.toString(), msgfmt);
            System.out.println("短信发送结果 " + result);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    //    单客户发送短信方法
    public static void assignXX_only_dx(String sjhm, String message) {
        System.out.println("message is " + message);
        System.out.println("进入发送短信逻辑");
        // 接口地址
        String ulr = "https://95386.njzq.cn:9000/sms/send.do?";
        // 客户ID，必填
        String userid = "400039";
        // 时间戳，必填
        String timespan = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());// 当前时间转化为yyyymmddhhmmss;
        // 原始密码，必填
        String password = "lrjk892022";
        // MD5加密后密码
        String pwd = getMD5(password + timespan);//原始密码+时间戳做MD5加密，32位大写格式
        // 手机号，必填
        String mobile = sjhm;
        // 内容，必填
        String content = message;
        // 扩展，选填，可为空，请先询问是否有扩展权限
        String ext = "";
        // 定时时间，选填，可为空
        String attime = "";
        // 选填，如果不填默认为GBK，可以选填GBK或者UTF8/UTF-8
        String msgfmt = "gbk";
        StringBuilder sendData = new StringBuilder();
        BASE64Encoder encoder = new BASE64Encoder();
        try {
            String content_base64 = encoder.encode(content.getBytes(msgfmt));// 做base64加密操作,编码方式使用msgfmt
            sendData.append("userid=").append(userid)
                    .append("&pwd=").append(pwd)
                    .append("&timespan=").append(timespan)
                    .append("&mobile=").append(mobile)
                    .append("&content=").append(content_base64)
                    .append("&ext=").append(ext)
                    .append("&attime=").append(attime);

            // 发送短信
            String result = httpPost(ulr, sendData.toString(), msgfmt);
            System.out.println("短信发送结果 " + result);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    /**
     * http协议提交
     *
     * @param ulr
     * @param data
     * @param charset
     * @return
     */
    public static String httpPost(String ulr, String data, String charset) {
        URL url = null;
        try {
            url = new URL(ulr);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded; charset=" + charset);
            conn.setConnectTimeout(30 * 1000);
            conn.setReadTimeout(30 * 1000);
            conn.setRequestMethod("POST");
            conn.setDoInput(true);
            conn.setDoOutput(true);

            OutputStream out = conn.getOutputStream();
            out.write(data.getBytes(charset));
            out.close();

            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), charset));
            StringBuilder response = new StringBuilder();
            String result;
            while (null != (result = in.readLine())) {
                response.append(result).append("\n");
            }
            in.close();
            return response.toString();
        } catch (MalformedURLException e) {
            e.printStackTrace();
            return "";
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }
    }

    /**
     * 方法名称：getMD5
     * 功    能：字符串MD5加密
     * 参    数：待转换字符串
     * 返 回 值：加密之后字符串
     */
    public static String getMD5(String sourceStr) {
        String resultStr = "";
        try {
            byte[] temp = sourceStr.getBytes();
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            md5.update(temp);
            // resultStr = new String(md5.digest());
            byte[] b = md5.digest();
            for (int i = 0; i < b.length; i++) {
                char[] digit = {'0', '1', '2', '3', '4', '5', '6', '7', '8',
                        '9', 'A', 'B', 'C', 'D', 'E', 'F'};
                char[] ob = new char[2];
                ob[0] = digit[(b[i] >>> 4) & 0X0F];
                ob[1] = digit[b[i] & 0X0F];
                resultStr += new String(ob);
            }
            return resultStr;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
    }

    //单客户发送钉钉方法
    public static void assignXX_only_dd_1(String userId, String message) {
        try {
            DingTalkClient client = new DefaultDingTalkClient("https://oapi.dingtalk.com/topapi/message/corpconversation/asyncsend_v2");
            OapiMessageCorpconversationAsyncsendV2Request request = new OapiMessageCorpconversationAsyncsendV2Request();
            request.setHttpMethod("POST");
            request.setAgentId(1390330698L);
            request.setUseridList(userId);
            request.setToAllUser(false);
            OapiMessageCorpconversationAsyncsendV2Request.Msg msg = new OapiMessageCorpconversationAsyncsendV2Request.Msg();
            msg.setMsgtype("markdown");
            msg.setMarkdown(new OapiMessageCorpconversationAsyncsendV2Request.Markdown());
            msg.getMarkdown().setText(message);
            msg.getMarkdown().setTitle("【南京证券-信用风险监控】");
//            msg.setMsgtype("text");
//            msg.setText(new OapiMessageCorpconversationAsyncsendV2Request.Text());
//            msg.getText().setContent(message);
            request.setMsg(msg);


            //卡片测试
//            msg.setActionCard(new OapiMessageCorpconversationAsyncsendV2Request.ActionCard());
//            msg.getActionCard().setTitle("xxx123411111");
//            msg.getActionCard().setMarkdown("### 测试123111");
//            msg.getActionCard().setSingleTitle("卡片测试------测试测试");
//            msg.getActionCard().setSingleUrl("https://www.dingtalk.com");
//            msg.setMsgtype("action_card");
//            request.setMsg(msg);
            //token有效期2个小时，需要不间断的获取
            OapiMessageCorpconversationAsyncsendV2Response rsp = client.execute(request, "18d7f2c5a0313c0192a7c31717cedcad");
            System.out.println(rsp.getBody());
        } catch (ApiException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static String getUserID_mobile(String sjhm) {
        DingTalkClient client = new DefaultDingTalkClient("https://oapi.dingtalk.com/topapi/v2/user/getbymobile");
        OapiV2UserGetbymobileRequest req = new OapiV2UserGetbymobileRequest();
        req.setMobile(sjhm);
        OapiV2UserGetbymobileResponse rsp = null;
        try {
            rsp = client.execute(req, "0f8952ff674c347cb084fbc7092c75e3");
        } catch (ApiException e) {
            e.printStackTrace();
        }
        return rsp.getResult().getUserid();
    }

    /**
     * 方法名称：getAccessToken
     * 功    能：由于token有效期为2个小时，获取钉钉token
     * 参    数：null
     * 返 回 值：token
     */
    public static String getAccessToken() throws Exception {
        String AppKey = "dingjht5yto6txlhej7h";
        String AppSecret = "onn5bPZ95U_149s6ebyv4hWzyhnECFSPjZGAfI-6LiAJPUsMzCEuCtHdIT4iykpC";
//        DefaultDingTalkClient client = new DefaultDingTalkClient("https://oapi.dingtalk.com/gettoken?appkey=" + AppKey + "?appsecret=" + AppSecret);
//        OapiGettokenRequest request = new OapiGettokenRequest();
//
//        request.setAppkey(AppKey);
//        request.setAppsecret(AppSecret);
//        request.setHttpMethod("GET");
//        OapiGettokenResponse response = client.execute(request);
//        String accessToken = response.getAccessToken();
        Properties prop = System.getProperties();
        // 设置http访问要使用的代理服务器的地址
        prop.setProperty("http.proxyHost", "10.254.255.55");
        prop.setProperty("http.proxyPort", "3128");
        // 对https也开启代理
        System.setProperty("https.proxyHost", "10.254.255.55");
        System.setProperty("https.proxyPort", "3128");
        //设置请求访问的地址
        URL url = new URL("https://oapi.dingtalk.com/gettoken?appkey=" + AppKey + "&appsecret=" + AppSecret);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setDoOutput(true);
        conn.setDoInput(true);
        conn.setUseCaches(false);
        conn.setRequestProperty("Connection", "Keep-Alive");
        conn.setRequestProperty("Charset", "UTF-8");
        conn.setConnectTimeout(60000);
        conn.setReadTimeout(60000);
        // 设置文件类型:
        conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
        // 设置接收类型否则返回415错误
        //conn.setRequestProperty("accept","*/*")此处为暴力方法设置接受所有类型，以此来防范返回415;
        conn.setRequestProperty("accept", "application/json");
        conn.connect();
        int HttpResult = conn.getResponseCode();
        String accessToken = null;
        JSONObject jsonObject = null;
        if (HttpResult == HttpURLConnection.HTTP_OK) {
            BufferedReader br = new BufferedReader(new InputStreamReader(
                    conn.getInputStream(), "utf-8"));
            String line = null;
            StringBuffer sb = new StringBuffer("");
            while ((line = br.readLine()) != null) {
                sb.append(line + "\n");
                jsonObject = JSONObject.parseObject(line);
            }
//            System.out.println(sb);
            br.close();

            accessToken = (String) jsonObject.get("access_token");
            //System.out.println(""+sb.toString());

        } else {
            System.out.println(conn.getResponseMessage());
        }


        return accessToken;
    }


    /**
     * 方法名称：assignXX_only_dd
     * 功    能：维持担保比例、合同到期告警
     * 参    数：accessToken，user_id，cotent
     * 返 回 值：null
     */
    public static void assignXX_only_dd(String accessToken, String user_id, String cotent) {
//        String accessToken = getAccessToken();
        Properties prop = System.getProperties();
        // 设置http访问要使用的代理服务器的地址
        prop.setProperty("http.proxyHost", "10.254.255.55");
        prop.setProperty("http.proxyPort", "3128");
        // 对https也开启代理
        System.setProperty("https.proxyHost", "10.254.255.55");
        System.setProperty("https.proxyPort", "3128");
        //设置请求访问的地址
        HttpURLConnection conn = null;
        try {
            URL url = new URL("https://oapi.dingtalk.com/topapi/message/corpconversation/asyncsend_v2?access_token=" + accessToken);
            conn = (HttpURLConnection) url.openConnection();
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

            jsonParam1.put("title", "【南京证券-信用风险监控】");
            jsonParam1.put("text", cotent);
            jsonParam3.put("markdown", jsonParam1);
            jsonParam3.put("msgtype", "markdown");
            jsonParam.put("agent_id", 1390330698L);
            jsonParam.put("userid_list", user_id);
            jsonParam.put("msg", jsonParam3);

            String jsonStr = JSONObject.toJSONString(jsonParam);
            logger.info(jsonStr);
            DataOutputStream printout;
            DataInputStream input;

            printout = new DataOutputStream(conn.getOutputStream());
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
                    logger.info(sb);
                }
                br.close();
            } else {
                logger.info(conn.getResponseMessage());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 任务下发时发送信息
     * */


    /**
     * 方法名称：assignXX_only_dd_task
     * 功    能：任务下发时发送信息
     * 参    数：user_id，yyb
     * 返 回 值：null
     */
    public static StringBuffer assignXX_only_dd_task(String user_id, String yyb) {
        StringBuffer sb = new StringBuffer("");
        try {
            String string = null;
            String yybmc = yyb;
            String task_rece = user_id;
            String accessToken = null;

            accessToken = getAccessToken();

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
            jsonParam1.put("title", "【南京证券-信用风险监控】");
            SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String dateStr = dateformat.format(System.currentTimeMillis());
            jsonParam1.put("text", dateStr + "  " + yybmc + ",你部有一条任务待处理，请前往风险监控平台——任务列表查看，并及时填写反馈 ");
            jsonParam3.put("markdown", jsonParam1);
            jsonParam3.put("msgtype", "markdown");


            jsonParam.put("agent_id", 1390330698L);
            jsonParam.put("userid_list", task_rece);
            jsonParam.put("msg", jsonParam3);

            String jsonStr = JSONObject.toJSONString(jsonParam);
            System.out.println("入参为: " + jsonStr);
            DataOutputStream printout;
            DataInputStream input;

            printout = new DataOutputStream(conn.getOutputStream());
            printout.write(jsonStr.getBytes());
            printout.flush();
            printout.close();
            int HttpResult = conn.getResponseCode();
            System.out.println("请求结果：" + HttpResult);
            if (HttpResult == HttpURLConnection.HTTP_OK) {
                BufferedReader br = new BufferedReader(new InputStreamReader(
                        conn.getInputStream(), "utf-8"));
                String line = null;
                while ((line = br.readLine()) != null) {
                    sb.append(line + "\n");
                    System.out.println("返回结果: " + sb);
                }
                br.close();
            } else {
                System.out.println(conn.getResponseMessage());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sb;
    }


}

