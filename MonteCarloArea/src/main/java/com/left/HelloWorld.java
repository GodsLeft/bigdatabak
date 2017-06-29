package com.left;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;

/**
 * Created by m2015 on 2017/6/4.
 * 框架启动器
 * 使用了MesosSchedulerDriver进行生命周期管理，需要一个框架调度器实例、一个描述框架信息的FrameworkInfo实例、Mesos master的地址，进行初始化
 */
public class HelloWorld {
    public static void main(String[] args) {
        System.out.println("Starting the MonteCarloArea on Mesos with master ");
        // Protos.FrameworkInfo frameworkInfo = Protos.FrameworkInfo.newBuilder().setName("MonteCarloArea").build();
        // MesosSchedulerDriver schedulerDriver = new MesosSchedulerDriver(new MonteCarloScheduler(), frameworkInfo, args[0]);
        // schedulerDriver.run();
    }
}
