package com.left;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedInputStream;
import java.net.URL;

/**
 * Created by m2015 on 2017/6/5.
 * 这是一个下载服务，跟Mesos当中的计算模块类似
 * 这个服务将会分配到每一个节点运行
 * 框架的计算任务
 */
public class DownloadFileService {
    private String fileURL = null;
    private String hdfsRootPath = null;

    private FileSystem fs = null;
    private Path outFile = null;
    private BufferedInputStream in = null;
    private FSDataOutputStream out = null;

    public DownloadFileService(String fileURL, String hdfsRootPath) throws Exception {
        this.fileURL = fileURL;
        this.hdfsRootPath = hdfsRootPath;
        String p = DownloadUtilities.getFilePathFromURL(hdfsRootPath, this.fileURL); // 获得下载的fileURL文件将要存放的位置
        this.outFile = new Path(p);  // 这里的outFile指的就是hdfs的文件
        Configuration conf = new Configuration();
        this.fs = FileSystem.get(conf);
    }

    public void initializeDownload() throws Exception {
        this.in = new BufferedInputStream(new URL(this.fileURL).openStream());
        this.out = fs.create(outFile);
    }

    public void closeAll() throws Exception {
        if (in != null) in.close();
        if (out != null) out.close();
        if(fs != null) fs.close();
    }

    public void downloadAndSaveFileFromUrl() throws Exception {
        try {
            byte data[] = new byte[1024];
            int count;
            while ((count = in.read(data, 0, 1024)) != -1)
                out.write(data, 0, count);
        } finally {
            this.closeAll();
        }
    }

    public void performDownloadSteps() throws Exception {
        if(DownloadUtilities.doesFileExists(this.fs, this.outFile)) {
            System.out.println(this.fileURL + " file is already downloaded");
            return;
        } else {
            this.initializeDownload();
            this.downloadAndSaveFileFromUrl();
        }
    }

    public static void main(String[] args) throws Exception {
        DownloadFileService service = null;
        try {
            String url = args[0].trim();
            String rootHDFSPath = args[1];
            service = new DownloadFileService(url, rootHDFSPath);
            service.performDownloadSteps();
        } finally {
            if(service != null)
                service.clone();
        }
    }
}
