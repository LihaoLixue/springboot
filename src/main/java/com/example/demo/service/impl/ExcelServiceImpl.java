//package com.example.demo.service.impl;
//
///**
// * @author LH
// * @description:
// * @date 2021-12-30 18:21
// */
//
//import com.example.demo.dao.UserMapper;
//import com.example.demo.model.Excel;
//import com.example.demo.service.ExcelService;
//import org.apache.poi.hssf.usermodel.*;
//import org.apache.poi.ss.usermodel.*;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Service;
//import org.springframework.web.multipart.MultipartFile;
//import org.apache.poi.hssf.usermodel.HSSFCellStyle;
//
//import javax.servlet.http.HttpServletResponse;
//import java.io.IOException;
//import java.io.OutputStream;
//import java.text.SimpleDateFormat;
//import java.util.*;
//
//@Service
//public class ExcelServiceImpl implements ExcelService {
//    @Autowired
//    private UserMapper userMapper;
//
//    @Override
//    public void exportExcel(HttpServletResponse response) throws IOException {
//        List<Excel> list_1 = userMapper.getAllUser();
//        HSSFWorkbook wb = new HSSFWorkbook();
//        // 第二步，在webbook中添加一个sheet,对应Excel文件中的sheet
//        HSSFSheet sheet = wb.createSheet("test");
//        // 第三步，在sheet中添加表头第0行,注意老版本poi对Excel的行数列数有限制short
//        HSSFRow row = sheet.createRow(0);
//        // 第四步，创建单元格，并设置值表头 设置表头居中
//        HSSFCellStyle style = wb.createCellStyle();
//        // 创建一个居中格式
//        style.setAlignment(HorizontalAlignment.CENTER);
//        /*此处根据情况自己自定义样式*/
//        HSSFCell cell = row.createCell(0);
//        cell.setCellValue("监控项目");
//        cell.setCellStyle(style);
//        cell = row.createCell(1);
//        cell.setCellValue("监控项目");
//        cell.setCellStyle(style);
//        cell = row.createCell(2);
//        cell.setCellValue("客户号");
//        cell.setCellStyle(style);
//        cell = row.createCell(3);
//        cell.setCellValue("客户姓名");
//        cell.setCellStyle(style);
//        cell = row.createCell(4);
//        cell.setCellValue("盘后维持担保比例");
//        cell.setCellStyle(style);
//        cell = row.createCell(5);
//        cell.setCellValue("盘中最低维持担保比例");
//        cell.setCellStyle(style);
//        cell = row.createCell(6);
//        cell.setCellValue("关注类证券集中度");
//        cell.setCellStyle(style);
//        cell = row.createCell(7);
//        cell.setCellValue("负债总额");
//        cell.setCellStyle(style);
//        cell = row.createCell(8);
//        cell.setCellValue("净资产净空头集中度");
//        cell.setCellStyle(style);
//        cell = row.createCell(9);
//        cell.setCellValue("客户沟通情况");
//        cell.setCellStyle(style);
//        int j = 0;
//        for (int i = 0; i < list_1.size(); i++) {
//            Excel dx = list_1.get(i);
//            if (dx.getYyb().equals("0010")) {
//                if (dx.getType().equals("2")) {
//                    j=j+1;
//                    row = sheet.createRow(j);
//                    // 第五步，写入实体数据 实际应用中这些数据从数据库得到，
//                    // 创建单元格，并设置值
//                    row.createCell(0).setCellValue("合约即将于1个交易日内到期");
//                    row.createCell(1).setCellValue("合约即将于1个交易日内到期");
//                    row.createCell(2).setCellValue(dx.getKhh());
//                    row.createCell(3).setCellValue("姓名");
//                    row.createCell(4).setCellValue(dx.getWbqj());
//                    row.createCell(5).setCellValue(dx.getWbqj());
//                    row.createCell(6).setCellValue(dx.getWbqj());
//                    row.createCell(7).setCellValue(dx.getWbqj());
//                    row.createCell(8).setCellValue(dx.getWbqj());
//                    row.createCell(9).setCellValue(dx.getWbqj());
//                }else if(dx.getType().equals("1")){
//                    j=j+1;
//                    row = sheet.createRow(j);
//                    // 第五步，写入实体数据 实际应用中这些数据从数据库得到，
//                    // 创建单元格，并设置值
//                    row.createCell(0).setCellValue("特别注意是否与客户约定特殊维保");
//                    row.createCell(1).setCellValue("维持担保比例状态");
//                    row.createCell(2).setCellValue(dx.getKhh());
//                    row.createCell(3).setCellValue("姓名");
//                    row.createCell(4).setCellValue(dx.getWbqj());
//                    row.createCell(5).setCellValue(dx.getWbqj());
//                    row.createCell(6).setCellValue(dx.getWbqj());
//                    row.createCell(7).setCellValue(dx.getWbqj());
//                    row.createCell(8).setCellValue(dx.getWbqj());
//                    row.createCell(9).setCellValue(dx.getWbqj());
//
//                }
//            }
//
//        }
//        //第六步,输出Excel文件
//        OutputStream output = response.getOutputStream();
//        response.reset();
//        //设置日期格式
//        SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
//        // 获取当前系统时间
//        String fileName = df.format(new Date());
//        //设置导出文件表头（即文件名）
//        response.setHeader("Content-disposition", "attachment; filename=" + fileName + ".xls");
//        //设置返回内容类型
//        response.setContentType("application/msexcel");
//        wb.write(output);
//        output.close();
//        // 第一步，创建一个webbook，对应一个Excel文件
//
//    }
//
//    private Map<HSSFWorkbook, HSSFSheet> getHSSFWorkbook() {
//        Map<HSSFWorkbook, HSSFSheet> map = new HashMap<>();
//        HSSFWorkbook wb = new HSSFWorkbook();
//        // 第二步，在webbook中添加一个sheet,对应Excel文件中的sheet
//        HSSFSheet sheet = wb.createSheet("test");
//        // 第三步，在sheet中添加表头第0行,注意老版本poi对Excel的行数列数有限制short
//        HSSFRow row = sheet.createRow(0);
//        // 第四步，创建单元格，并设置值表头 设置表头居中
//        HSSFCellStyle style = wb.createCellStyle();
//        // 创建一个居中格式
//        style.setAlignment(HorizontalAlignment.CENTER);
//        /*此处根据情况自己自定义样式*/
//        HSSFCell cell = row.createCell(0);
//        cell.setCellValue("监控项目");
//        cell.setCellStyle(style);
//        cell = row.createCell(1);
//        cell.setCellValue("监控项目");
//        cell.setCellStyle(style);
//        cell = row.createCell(2);
//        cell.setCellValue("客户号");
//        cell.setCellStyle(style);
//        cell = row.createCell(3);
//        cell.setCellValue("客户姓名");
//        cell.setCellStyle(style);
//        cell = row.createCell(4);
//        cell.setCellValue("盘后维持担保比例");
//        cell.setCellStyle(style);
//        cell = row.createCell(5);
//        cell.setCellValue("盘中最低维持担保比例");
//        cell.setCellStyle(style);
//        cell = row.createCell(6);
//        cell.setCellValue("关注类证券集中度");
//        cell.setCellStyle(style);
//        cell = row.createCell(7);
//        cell.setCellValue("负债总额");
//        cell.setCellStyle(style);
//        cell = row.createCell(8);
//        cell.setCellValue("净资产净空头集中度");
//        cell.setCellStyle(style);
//        cell = row.createCell(9);
//        cell.setCellValue("客户沟通情况");
//        cell.setCellStyle(style);
////            map.put("1",wp);
//        return map;
//    }
//}
