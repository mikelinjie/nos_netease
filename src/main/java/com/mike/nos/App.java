package com.mike.nos;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.ResourceBundle;
import java.util.logging.Logger;

/**
 * Hello world!
 */
public class App {

    private static SimpleDateFormat sdf    = new SimpleDateFormat("yyyyMMddHHmmss");
    private static Logger           logger = Logger.getLogger("App.class");

    public static void main(String[] args) {
        ResourceBundle rb = ResourceBundle.getBundle("application");
        String bktn = rb.getString("bucketName");
        for (String path : args) {
            File f = new File(path);
            try {
                if (f.exists()) NosUtils.uploadFile(bktn, sdf.format(new Date()) + "_" + f.getName(), f);
                logger.info("上传[" + f.getName() + "]成功------");
            } catch (Exception e) {
                logger.warning("上传[" + f.getName() + "]失败------");
            }
        }
        NosUtils.shutdown();
    }
}
