package com.example.demo.controller;

import com.alibaba.fastjson.JSONObject;
import com.example.demo.service.LogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * @author LH
 * @description: 文件上传与下载
 * @date 2022-06-28 17:25
 */
@RestController
@Scope("prototype")
@RequestMapping("/file")
public class FileController {

    @Autowired
    private LogService logService;

    /**
     * 下载日志接口
     *
     * @param name
     * @param response
     * @throws Exception
     */
    @GetMapping(value = "/download/{name}")
    public void logDownload(@PathVariable String name, HttpServletResponse response) throws Exception {
        logService.logDownload(name, response);
    }

    /**
     * 上传日志接口
     *
     * @param file
     * @return
     * @throws Exception
     */
    @PostMapping(value = "/upload")
    public JSONObject logUpload(@RequestParam("file") MultipartFile file) throws Exception {
        return logService.logUpload(file);
    }

    /**
     * 批量上传日志接口
     *
     * @param request
     * @return
     * @throws Exception
     */
    @PostMapping(value = "/uploads")
    public JSONObject logUploads(HttpServletRequest request) throws Exception {
        return logService.logUploads(request);
    }
    /**
     * 文件删除
     *
     * @param fileName
     * @return
     * @throws Exception
     */
    @PostMapping(value = "/delete")
    public JSONObject deleteFlile(@RequestParam("text") String fileName) throws Exception {
        System.out.println("222222222222"+fileName);
        return logService.deleteAssignAnnex(fileName);
    }
}
