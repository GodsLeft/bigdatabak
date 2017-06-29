package com.left;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by m2015 on 2017/6/5.
 */
public class DownloadUtilities {
    public static boolean doesFileExists(FileSystem fs, Path outFile) throws Exception {
        return fs.exists(outFile);
    }

    // 返回的是下载文件在hdfs当中的存储位置
    // rootPath是hdfs根目录
    public static String getFilePathFromURL(String rootPath, String fileUrl) {
        String filePath = rootPath + "/" + fileUrl.substring(fileUrl.lastIndexOf("/")+1);
        return filePath;
    }

    // 将下载文件地址转化为List
    // 下载文件地址应该是存放在hdfs上
    public static List<String> getFileListing(String patentListFile) throws Exception {
        List<String> lines = new ArrayList<String>();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path p = new Path(patentListFile);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
        String line;
        line = br.readLine();
        System.out.println(line);
        if(line.length() >= 2){
            lines.add(line);
        }
        while(line != null){
            line = br.readLine();
            if(line.trim().length() >= 0){
                lines.add(line);
            }
        }
        return lines;
    }
}
