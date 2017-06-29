package com.left;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateRequestPBImpl;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

import java.util.Collections;
import java.util.List;

/**
 * Created by m2015 on 2017/6/5.
 * 1.初始化应用管理协议和容器管理协议
 * 2.使用应用管理协议在资源ResourceManager当中注册应用管理器
 * 3.配置容器用以启动工作任务的参数
 * 4.通过应用管理协议请求容器。这一步会获得所有已分配容器的引用
 * 5.使用容器管理协议启动工作任务
 * 6.等待所有任务完成
 * 7.所有容器都返回后，在资源管理器中注销应用管理器
 */
public class ApplicationMaster {
    public static void main(String[] args) throws Exception {
        final String hdfsPathToPatentFiles = args[0];
        final String destHdfsFolder = args[1];
        List<String> files = DownloadUtilities.getFileListing(hdfsPathToPatentFiles);
        String command = "";
        Configuration conf = new YarnConfiguration();
        AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient(); // 应用管理协议
        rmClient.init(conf);
        rmClient.start();

        NMClient nmClient = NMClient.createNMClient(); // 容器管理协议
        nmClient.init(conf);
        nmClient.start();

        // 在resourceManager中注册应用管理器
        rmClient.registerApplicationMaster("", 0, "");
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        // 配置容器参数
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(128);
        capability.setVirtualCores(1);

        // 制作资源请求向ResourceManager申请
        // 向资源管理器请求容器
        for(int i=0; i<files.size(); ++i){
            ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
            rmClient.addContainerRequest(containerAsk);
        }

        // 获得分配的容器并加载任务
        int allocatedContainers = 0;
        while(allocatedContainers < files.size()) {
            AllocateResponse response = rmClient.allocate(0);
            for(Container container : response.getAllocatedContainers()){
                command = "java com.left.DownloadFileService " + files.get(allocatedContainers) + " " + destHdfsFolder;
                ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
                ctx.setCommands(Collections.singletonList(command));
                nmClient.startContainer(container, ctx);
                ++allocatedContainers;
            }
            Thread.sleep(100);
        }

        // 等待容器完成
        int completedContainers = 0;
        while(completedContainers < files.size()) {
            AllocateResponse response = rmClient.allocate(completedContainers/files.size());
            for(ContainerStatus status : response.getCompletedContainersStatuses()) {
                ++ completedContainers;
            }
            Thread.sleep(100);
        }
        rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
    }
}
