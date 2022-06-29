package com.example.demo.service;

import com.alibaba.fastjson.JSONObject;
import com.example.demo.exception.GlobalException;
import com.example.demo.model.ReturnMessage;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.MultipartHttpServletRequest;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.List;

/**
 * @author LH
 * @description: 日志服务接口
 * @date 2022-06-28 17:26
 */
@Service
public class LogService {

    public void logDownload(String name, HttpServletResponse response) throws Exception {
        File file = new File("log" + File.separator + name);

        if (!file.exists()) {
            throw new GlobalException(name + "文件不存在");
        }
        response.setContentType("application/force-download");
        response.addHeader("Content-Disposition", "attachment;fileName=" + name);
        response.setContentLength((int) file.length());

        byte[] buffer = new byte[1024];
        try (FileInputStream fis = new FileInputStream(file);
             BufferedInputStream bis = new BufferedInputStream(fis)) {
            OutputStream os = response.getOutputStream();

            int i = bis.read(buffer);
            while (i != -1) {
                os.write(buffer, 0, i);
                i = bis.read(buffer);
            }
        }
    }

    public JSONObject logUpload(MultipartFile file) throws Exception {
        if (file == null || file.isEmpty()) {
            throw new GlobalException("未选择需上传的日志文件");
        }

        String filePath = new File("logs_app").getAbsolutePath();
        System.out.println("12345"+filePath);
        File fileUpload = new File(filePath);
        if (!fileUpload.exists()) {
            fileUpload.mkdirs();
        }

        fileUpload = new File(filePath, file.getOriginalFilename());
        if (fileUpload.exists()) {
            throw new GlobalException("上传的日志文件已存在");
        }

        try {
            file.transferTo(fileUpload);

            return ReturnMessage.success();
        } catch (IOException e) {
            throw new GlobalException("上传日志文件到服务器失败：" + e.toString());
        }
    }

    public JSONObject logUploads(HttpServletRequest request) throws Exception {
        List<MultipartFile> files = ((MultipartHttpServletRequest) request).getFiles("file");

        for (MultipartFile file : files) {
            logUpload(file);
        }

        return ReturnMessage.success();
    }

    public JSONObject deleteAssignAnnex(String fileName) {
        String filePath = new File("logs_app").getAbsolutePath();
        System.out.println("333333 "+filePath);
        System.out.println("444444 "+fileName);
        File fileDelete = new File(filePath,fileName);
        boolean delete = fileDelete.delete();
        if (!delete){
            throw new RuntimeException("删除失败，请联系管理员");
        }
        return ReturnMessage.success();
    }
}
