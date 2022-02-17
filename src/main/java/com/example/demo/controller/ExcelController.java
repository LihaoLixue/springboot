//package com.example.demo.controller;
//
//
//import com.example.demo.service.ExcelService;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Controller;
//import org.springframework.ui.Model;
//import org.springframework.web.bind.annotation.GetMapping;
//import org.springframework.web.bind.annotation.PostMapping;
//import org.springframework.web.bind.annotation.RequestMapping;
//import org.springframework.web.multipart.MultipartFile;
//
//import javax.servlet.http.HttpServletResponse;
//import javax.xml.soap.SAAJResult;
//
//@Controller
//public class ExcelController {
//
//    @Autowired
//    private ExcelService excelService;
//
//    @GetMapping("/export")
//    public void exportExcel(HttpServletResponse response) {
//        try {
//            excelService.exportExcel(response);
//            System.out.println("导出成功");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//}
