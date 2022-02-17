package com.example.demo.service;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author LH
 * @description:
 * @date 2021-12-30 18:21
 */
public interface ExcelService {
    void exportExcel(HttpServletResponse response) throws IOException;
}
