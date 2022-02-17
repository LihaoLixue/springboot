package com.example.demo.result;

/**
 * @author LH
 * @description:
 * @date 2021-07-23 16:00
 */

public class Result {
    //响应码
    private int code;
    private String message;
    private Object result;

    Result(int code, String message, Object data) {
        this.code = code;
        this.message = message;
        this.result = data;
    }
    public Result(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

}

