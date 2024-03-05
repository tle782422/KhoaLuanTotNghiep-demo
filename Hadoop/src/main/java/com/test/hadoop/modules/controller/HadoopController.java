package com.test.hadoop.modules.controller;

import com.google.gson.Gson;
import com.test.hadoop.modules.service.HadoopOperateService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/hdfs")
public class HadoopController {

    @Autowired
    private HadoopOperateService operateService;

    private final Gson gson = new Gson();

    @GetMapping("/upload")
    public ModelAndView uploadPage() {
        return new ModelAndView("upload");
    }

    @GetMapping("")
    public ModelAndView index() {
        return new ModelAndView("index");
    }

    @PostMapping("/uploadFile")
    @ResponseBody
    public ResponseEntity<String> uploadFile(@RequestPart MultipartFile file) throws Exception {
        operateService.uploadFile(file);
        return ResponseEntity.ok("{\"result\": \"success\"}");
    }

    @GetMapping("/findByPath")
    @ResponseBody
    public ResponseEntity<String> findByPath(String path) {
        List<Map<String, String>> all = operateService.findAll(path);
        String allData = gson.toJson(all);
        return ResponseEntity.ok(allData);
    }

    @PostMapping("/deleteFile")
    @ResponseBody
    public ResponseEntity<Boolean> deleteFile(@RequestParam String path) throws Exception {
        return ResponseEntity.ok(operateService.deleteFile(path));
    }

    @GetMapping("/downloadFile")
    public ResponseEntity<byte[]> downloadFile(@RequestParam String path) {
        try {
            byte[] fileContent = operateService.getFileContent(path);
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
            headers.setContentDispositionFormData("attachment", path);

            return ResponseEntity.ok()
                    .headers(headers)
                    .body(fileContent);
        } catch (IOException e) {
            e.printStackTrace();
            return ResponseEntity.status(500).build();
        }
    }

}
