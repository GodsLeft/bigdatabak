package com.left;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by m2015 on 2017/6/5.
 */
public class Client {
    Configuration conf = new YarnConfiguration();

    public void run(String[] args) throws Exception {
        final String hdfsPathToPatentFileList = args[0];
        final String hdfsOutFolder = args[1];
        final Path jarPath = new Path(args[2]);

        // 创建YarnClient
        YarnConfiguration conf = new YarnConfiguration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();

        // 创建应用 通过yarnClient
        YarnClientApplication app = yarnClient.createApplication();

        // 设置容器加载上下文为 application master
        // 包含了NodeManager启动容器所需的所有信息
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
        amContainer.setCommands(Collections.singletonList(" java " + " -Xmx128m" + " com.left.ApplicationMaster " + hdfsPathToPatentFileList + " " + hdfsOutFolder));

        // 设置jar为 applicationMaster
        LocalResource appMasterJar = setupAppMasterJar(jarPath);
        amContainer.setLocalResources(Collections.singletonMap("appmaster.jar", appMasterJar));

        // 设置CLASSPATH for applicationMaster
        Map<String, String> appMasterEnv = new HashMap<String, String>();
        setupAppMasterEnv(appMasterEnv);
        amContainer.setEnvironment(appMasterEnv);

        // 设置资源类型需求 for applicationMaster
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(256);
        capability.setVirtualCores(1);

        // set-up ApplicationSubmissionContext for the application
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        appContext.setApplicationName("download-yarn-app");
        appContext.setAMContainerSpec(amContainer);
        appContext.setQueue("default");

        // 提交应用
        ApplicationId appId = appContext.getApplicationId();
        System.out.println("submitting application " + appId);
        yarnClient.submitApplication(appContext);

        ApplicationReport appReport = yarnClient.getApplicationReport(appId);
        YarnApplicationState appState = appReport.getYarnApplicationState();
        while(appState != YarnApplicationState.FINISHED && appState != YarnApplicationState.KILLED && appState != YarnApplicationState.FAILED) {
            Thread.sleep(100);
            appReport = yarnClient.getApplicationReport(appId);
            appState = appReport.getYarnApplicationState();
        }

        System.out.println("Application " + appId + " finished with" + " state " + appState + "." + " The application started at " + appReport.getStartTime() + "." + " The Application ended at " + appReport.getFinishTime());
    }

    private LocalResource setupAppMasterJar(Path jarPath) throws IOException {
        LocalResource appMasterJar = Records.newRecord(LocalResource.class);
        FileStatus jarStat = FileSystem.get(conf).getFileStatus(jarPath);
        appMasterJar.setType(LocalResourceType.FILE);
        appMasterJar.setVisibility(LocalResourceVisibility.APPLICATION);
        appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        appMasterJar.setSize(jarStat.getLen());
        appMasterJar.setTimestamp(jarStat.getModificationTime());
        return appMasterJar;
    }

    private void setupAppMasterEnv(Map<String, String> appMasterEnv) {
        for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            Apps.addToEnvironment(appMasterEnv, ApplicationConstants.Environment.CLASSPATH.name(), c.trim());
        }
        Apps.addToEnvironment(appMasterEnv, ApplicationConstants.Environment.CLASSPATH.name(), ApplicationConstants.Environment.PWD.$() + File.separator + "*");
    }
    public static void main(String[] args) throws Exception {
        Client c = new Client();
        c.run(args);
    }
}
