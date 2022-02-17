package com.example.demo.until.ddmessage;


import com.alibaba.fastjson.JSONObject;

/**
 * @author LH
 * @description:
 * @date 2021-12-06 16:51
 */
public class main {
    public static void main(String[] args) {
        JSONObject jsonParam = new JSONObject();
        JSONObject jsonParam1 = new JSONObject();
        JSONObject jsonParam2 = new JSONObject();
        JSONObject jsonParam3 = new JSONObject();

        jsonParam1.put("content","atsaasf1111");
        jsonParam2.put("text",jsonParam1);
        jsonParam3.put("msgtype","text");
        jsonParam3.put("text",jsonParam1);



        jsonParam.put("agent_id", "1390330698L");
        jsonParam.put("userid_list", "002968");
        jsonParam.put("msg",jsonParam3);
        System.out.println(jsonParam);
    }
}
