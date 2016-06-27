package com.mike.nos;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Date;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;

import com.netease.cloud.auth.BasicCredentials;
import com.netease.cloud.auth.Credentials;
import com.netease.cloud.auth.PropertiesCredentials;
import com.netease.cloud.services.nos.Headers;
import com.netease.cloud.services.nos.NosClient;
import com.netease.cloud.services.nos.internal.Constants;
import com.netease.cloud.services.nos.model.GeneratePresignedUrlRequest;
import com.netease.cloud.services.nos.model.GetObjectRequest;
import com.netease.cloud.services.nos.model.NOSObject;
import com.netease.cloud.services.nos.model.ObjectMetadata;
import com.netease.cloud.services.nos.transfer.TransferManager;
import com.netease.cloud.services.nos.transfer.Upload;
import com.netease.cloud.services.nos.transfer.model.UploadResult;

public class NosUtils {

    public static String nosHostName = "cloud.126.net";
    static {
        ResourceBundle rb = ResourceBundle.getBundle("application");
        NosUtils.nosHostName = rb.getString("hostName");
        String accessKey = rb.getString("accessKey");
        String secretKey = rb.getString("secretKey");
        NosUtils.init(accessKey, secretKey);
    }

    private static Logger         logger           = Logger.getLogger("NosUtils.class");

    public static NosClient       nosClient;
    public static TransferManager tranManager;
    public static String          DEFAULT_ENCODING = "UTF-8";

    public static void init(InputStream is) throws IOException {
        initConstants();
        tranManager = new TransferManager(new PropertiesCredentials(is));
        nosClient = (NosClient) tranManager.getNosClient();
    }

    public static void init(String accessKey, String secretKey) {
        initConstants();
        Credentials cred = new BasicCredentials(accessKey, secretKey);
        tranManager = new TransferManager(cred);
        nosClient = (NosClient) tranManager.getNosClient();
    }

    public static String uploadFile(String bucketName, String key, File file) {
        try {
            Upload upload = tranManager.upload(bucketName, key, file);

            UploadResult result = upload.waitForUploadResult();
            logger.info("bucketName:=" + result.getBucketName() + ",eTag:=" + result.getETag() + ",key:="
                        + result.getKey() + ",versionId:=" + result.getVersionId());
        } catch (Exception ex) {
            logger.warning("uploadFileError" + ex);
            return "uploadFileError";
        }
        return "uploadSuccess";
    }

    public static String uploadFile(String bucketName, String key, InputStream input) {
        try {
            Upload upload = tranManager.upload(bucketName, key, input, null);
            UploadResult result = upload.waitForUploadResult();
            logger.info("bucketName:=" + result.getBucketName() + ",eTag:=" + result.getETag() + ",key:="
                        + result.getKey() + ",versionId:=" + result.getVersionId());
        } catch (Exception ex) {
            logger.warning("uploadFileError");
            return "uploadFileError";
        }
        return "uploadFileSuccess";
    }

    public static String uploadFile(String bucketName, String key, InputStream input, long size) {
        try {
            ObjectMetadata meta = new ObjectMetadata();
            meta.setHeader(Headers.CONTENT_LENGTH, size);
            Upload upload = tranManager.upload(bucketName, key, input, meta);
            UploadResult result = upload.waitForUploadResult();
            logger.info("bucketName:=" + result.getBucketName() + ",eTag:=" + result.getETag() + ",key:="
                        + result.getKey() + ",versionId:=" + result.getVersionId());
        } catch (Exception ex) {
            logger.warning("uploadFileError");
            return "uploadFileError";
        }
        return "uploadFileSuccess";
    }

    public static String uploadFile(String bucketName, String key, InputStream input, ObjectMetadata meta) {
        try {
            Upload upload = tranManager.upload(bucketName, key, input, meta);
            UploadResult result = upload.waitForUploadResult();
            logger.info("bucketName:=" + result.getBucketName() + ",eTag:=" + result.getETag() + ",key:="
                        + result.getKey() + ",versionId:=" + result.getVersionId());
        } catch (Exception ex) {
            logger.warning("uploadFileError");
            return "uploadFileError";
        }
        return "uploadFileSuccess";
    }

    public static InputStream downloadFile(String bucketName, String key) {
        InputStream is = null;
        try {
            NOSObject object = nosClient.getObject(new GetObjectRequest(bucketName, key));
            is = object.getObjectContent();
        } catch (Exception ex) {
            logger.warning("downloadFileError");
        }
        return is;
    }

    /**
     * 下载到指定路径下
     * 
     * @param bucketName
     * @param key
     * @param filePath
     * @return
     */
    public static String downloadFile(String bucketName, String key, String filePath) {
        InputStream input = NosUtils.downloadFile(bucketName, key);
        int byteread = 0;
        FileOutputStream fs = null;
        try {
            fs = new FileOutputStream(filePath);
            byte[] buffer = new byte[input.available()];
            while ((byteread = input.read(buffer)) != -1) {
                fs.write(buffer, 0, byteread);
            }
        } catch (Exception e) {
            logger.warning("download file error from NOS");
        } finally {
            IOUtils.closeQuietly(input);
            IOUtils.closeQuietly(fs);
        }
        return "download file error from NOS";
    }

    /**
     * 下载文件(指定range)
     * 
     * @param bucketName
     * @param key
     * @param start
     * @param end
     * @return
     */
    public static InputStream downloadFileByRange(String bucketName, String key, long start, long end) {
        InputStream is = null;
        try {
            GetObjectRequest request = new GetObjectRequest(bucketName, key);
            request.setRange(start, end);
            NOSObject object = nosClient.getObject(request);
            is = object.getObjectContent();
        } catch (Exception ex) {
            logger.warning("downloadFileError");
        }
        return is;
    }

    public static String downloadFileAsString(String bucketName, String key) {
        return downloadFileAsString(bucketName, key, DEFAULT_ENCODING);
    }

    public static String downloadFileAsString(String bucketName, String key, String encoding) {
        String ret = null;
        BufferedInputStream bis = null;
        try {
            bis = new BufferedInputStream(downloadFile(bucketName, key));
            byte[] buffer = new byte[1024 * 1024 * 4];
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            int readBytes = 0;
            while ((readBytes = bis.read(buffer)) != -1) {
                baos.write(buffer, 0, readBytes);
            }
            baos.flush();
            byte[] data = baos.toByteArray();
            ret = new String(data, encoding);
        } catch (Exception ex) {
            logger.warning("downloadFileAsStringError");
        } finally {
            IOUtils.closeQuietly(bis);
        }
        return ret;
    }

    /**
     * 获取对象的md5
     * 
     * @param bucketName
     * @param key
     * @return
     */
    public static String getObjectMd5(String bucketName, String key) {
        String ret = null;
        try {
            ObjectMetadata om = nosClient.getObjectMetadata(bucketName, key);
            ret = om.getETag();
        } catch (Exception ex) {
            logger.warning("getObjectMd5Error");
        }
        return ret;
    }

    @Deprecated
    public static String deleteFile(String bucketName, String key, String versionId) {
        try {
            nosClient.deleteObject(bucketName, key, versionId);
        } catch (Exception ex) {
            logger.warning("deleteFileError");
        }
        return "deleteFileError";
    }

    /**
     * 删除Nos上的对象
     * 
     * @param bucketName
     * @param key
     * @return
     */
    public static String deleteFile(String bucketName, String key) {
        try {
            nosClient.deleteObject(bucketName, key);
        } catch (Exception ex) {
            logger.warning("deleteFileError");
        }
        return "deleteFileError";
    }

    private static void initConstants() {
        if (nosHostName != null) {
            Constants.NOS_HOST_NAME = nosHostName;
        }
    }

    public static void setNosHostName(String nosHostName) {
        NosUtils.nosHostName = nosHostName;
    }

    public static void shutdown() {
        if (tranManager != null) {
            tranManager.shutdownNow();
        }
    }

    public static String getPrivateObjectUrl(String bucketName, String key, long expire) {
        GeneratePresignedUrlRequest generatePresignedUrlRequest = new GeneratePresignedUrlRequest(bucketName, key);
        generatePresignedUrlRequest.setExpiration(new Date(System.currentTimeMillis() + expire));
        URL url = nosClient.generatePresignedUrl(generatePresignedUrlRequest);
        return url.toString();
    }
}
