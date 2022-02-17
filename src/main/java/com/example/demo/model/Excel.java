package com.example.demo.model;

/**
 * @author LH
 * @description:
 * @date 2021-12-30 18:19
 */
public class Excel {
    private String id;
    private String khh;
    private String sj;
    private String wbqj;
    private String qylx;
    private String dqlx;
    private String type;
    private String rwzt;
    private String rq;
    private String title;
    private String zb;
    private String yyb;

    public Excel() {
    }

    public Excel(String id, String khh, String sj, String wbqj, String qylx, String dqlx, String type, String rwzt, String rq, String title, String zb, String yyb) {
        this.id = id;
        this.khh = khh;
        this.sj = sj;
        this.wbqj = wbqj;
        this.qylx = qylx;
        this.dqlx = dqlx;
        this.type = type;
        this.rwzt = rwzt;
        this.rq = rq;
        this.title = title;
        this.zb = zb;
        this.yyb = yyb;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getKhh() {
        return khh;
    }

    public void setKhh(String khh) {
        this.khh = khh;
    }

    public String getSj() {
        return sj;
    }

    public void setSj(String sj) {
        this.sj = sj;
    }

    public String getWbqj() {
        return wbqj;
    }

    public void setWbqj(String wbqj) {
        this.wbqj = wbqj;
    }

    public String getQylx() {
        return qylx;
    }

    public void setQylx(String qylx) {
        this.qylx = qylx;
    }

    public String getDqlx() {
        return dqlx;
    }

    public void setDqlx(String dqlx) {
        this.dqlx = dqlx;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getRwzt() {
        return rwzt;
    }

    public void setRwzt(String rwzt) {
        this.rwzt = rwzt;
    }

    public String getRq() {
        return rq;
    }

    public void setRq(String rq) {
        this.rq = rq;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getZb() {
        return zb;
    }

    public void setZb(String zb) {
        this.zb = zb;
    }

    public String getYyb() {
        return yyb;
    }

    public void setYyb(String yyb) {
        this.yyb = yyb;
    }
}
