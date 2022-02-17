package com.example.demo.until.scheduler;

import com.example.demo.until.ddmessage.AssignMessage;

import java.util.UUID;

import static com.example.demo.until.ddmessage.AssignMessage.getAccessToken;


public class UuidUtil {
    public static String get32UUID() {
        String uuid = UUID.randomUUID().toString().trim().replaceAll("-", "");
        return uuid;
    }
    public  static String getData_2(String str) {
        double v = Double.parseDouble(str);
        System.out.println(v);
        String format = String.format("%.2f", v);
        return format;
    }
    //合约到期告警信息配置
    public static String alertHtdqMessage(String khxx) {
        String[] split = khxx.split(",");
        String message_10="";
        String message_1="";
        String message_0="";
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
                    AssignMessage.assignXX_only_dd(accessToken,"002968", "数据格式错误，请检查。"+mess);
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
        String message="";
        if (message_10.length()>0&&message_0.length()>0) {
            String substring = message_0.substring(0, message_0.length() - 1);
            String substring10 = message_10.substring(0, message_10.length() - 1);
            message = "你部" +  substring+ "有融资融券合约将于今日到期;" + message_1 + substring10 + "  \n  有融资融券合约将于10个交易日到期，请前往风险监控平台——预警中心查看处理。";
        }else if(message_10.length()>0&&message_0.length()==0){
            String substring10 = message_10.substring(0, message_10.length() - 1);
            message = "你部" + message_1 + substring10+ "  \n  有融资融券合约将于10个交易日到期，请前往风险监控平台——预警中心查看处理。";
        }else if(message_10.length()==0&&message_0.length()==0&&message_1.length()>0){
            String substring1 = message_1.substring(0, message_1.length() - 1);
            message = "你部" + message_1.substring(0, message_1.length() - 1) + "  \n  有融资融券合约将于今日到期,请前往风险监控平台——预警中心查看处理。";
        }
        return message;
    }
}
