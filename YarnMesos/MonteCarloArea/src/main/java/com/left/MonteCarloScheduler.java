package com.left;

import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by m2015 on 2017/6/4.
 * 这是一个框架调度器
 */
public class MonteCarloScheduler implements Scheduler {
    private LinkedList<String> tasks;
    private int numTasks;
    private int tasksSubmitted;
    private int tasksCompleted;
    private double totalArea;
    private String JAR_PATH;

    public MonteCarloScheduler(){}
    public MonteCarloScheduler(String[] args, int numTasks, String JAR_PATH){
        this.numTasks = numTasks;
        tasks = new LinkedList<String>();
        double xLow = Double.parseDouble(args[1]);
        double xHigh = Double.parseDouble(args[2]);
        double yLow = Double.parseDouble(args[3]);
        double yHigh = Double.parseDouble(args[4]);

        double xStep = (xHigh - xLow) / (numTasks / 2); // numTasks越多，计算结果越精确
        double yStep = (yHigh - yLow) / (numTasks / 2);
        for(double x=xLow; x<xHigh; x+= xStep) {
            for (double y = yLow; y < yHigh; y += yStep) {
                tasks.add(" \"" + args[0] + " \"" + x + " " + (x + xStep) + " " + y + " " + (y+yStep)+" " + args[5]); // 疑问：args[0] 与 args[5]分别是什么
            }
        }
        this.JAR_PATH = JAR_PATH;
    }

    // 调度器向Mesos master注册时调用
    // FrameworkID：是Mesos提供的全局唯一ID
    public void registered(SchedulerDriver schedulerDriver, Protos.FrameworkID frameworkID, Protos.MasterInfo masterInfo) {
        System.out.println("Scheduler registered with id " + frameworkID.getValue() + " with tasks " + tasks);
    }

    // 框架调度器向新选举的master重新注册后调用，masterInfo中提供了新的master相关信息
    public void reregistered(SchedulerDriver schedulerDriver, Protos.MasterInfo masterInfo) {
        System.out.println("Scheduler re-registered");
    }

    // 调度器接口中最重要的一个
    // 在master向框架提供资源offer时调用，每一份资源offer包含从某个slave上获取的资源列表
    // 调度器可以接受资源offer并利用offerIds来启动任务
    public void resourceOffers(SchedulerDriver schedulerDriver, List<Protos.Offer> offers) {
        for (Protos.Offer offer : offers) {
            if(tasks.size() > 0) {
                tasksSubmitted ++;
                String task = tasks.remove();
                Protos.TaskID taskID = Protos.TaskID.newBuilder().setValue(String.valueOf(tasksSubmitted)).build();
                System.out.println("Launching task " + taskID.getValue() + " on slave " + offer.getSlaveId().getValue() + " with " + task);
                Protos.ExecutorInfo executor = Protos.ExecutorInfo.newBuilder()
                        .setExecutorId(Protos.ExecutorID.newBuilder().setValue(String.valueOf(tasksSubmitted)))
                        .setCommand(createCommand(task))
                        .setName("MonteCarlo Executor (Java)")
                        .build();

                Protos.TaskInfo taskInfo = Protos.TaskInfo.newBuilder()
                        .setName("MonteCarloTask-" + taskID.getValue())
                        .setTaskId(taskID)
                        .setExecutor(Protos.ExecutorInfo.newBuilder(executor))
                        .addResources(Protos.Resource.newBuilder()
                                .setName("cpus")
                                .setType(Protos.Value.Type.SCALAR)
                                .setScalar(Protos.Value.Scalar.newBuilder().setValue(1)))
                        .addResources(Protos.Resource.newBuilder()
                                .setName("mem")
                                .setType(Protos.Value.Type.SCALAR)
                                .setScalar(Protos.Value.Scalar.newBuilder().setValue(128)))
                        .setSlaveId(offer.getSlaveId())
                        .build();

                schedulerDriver.launchTasks(Collections.singletonList(offer.getId()), Collections.singletonList(taskInfo));
            }
        }
    }

    private Protos.CommandInfo.Builder createCommand(String args) {
        return Protos.CommandInfo.newBuilder().setValue("java -cp " + JAR_PATH + " com.left.MonteCarloExecutor " + args);
    }

    // master撤销发送给框架的资源offer时被调用
    public void offerRescinded(SchedulerDriver schedulerDriver, Protos.OfferID offerID) {

    }

    // 每当Mesos向框架发送消息时，该方法都会被调用
    public void statusUpdate(SchedulerDriver schedulerDriver, Protos.TaskStatus taskStatus) {
        System.out.println("Status update: task " + taskStatus.getTaskId().getValue() + " state is " + taskStatus.getState());
        if (taskStatus.getState().equals(Protos.TaskState.TASK_FINISHED)) {
            tasksCompleted ++;
            double area = Double.parseDouble(taskStatus.getData().toStringUtf8());
            totalArea += area;
            System.out.println("Task " + taskStatus.getTaskId().getValue() + " finished with area : " + area);
        } else {
            System.out.println("Task " + taskStatus.getTaskId().getValue() + " has message " + taskStatus.getMessage());
        }
        if(tasksCompleted == numTasks) {
            System.out.println("Total Area : " + totalArea);
            schedulerDriver.stop();
        }
    }

    // 用来向调度器传递执行器发送的消息
    // 调度器可以访问执行器和slave的ID，以及调度器所发送的数据
    // 消息传递采用基于最大努力交付
    public void frameworkMessage(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, byte[] bytes) { }

    //
    public void disconnected(SchedulerDriver schedulerDriver) { }

    // 通知调度器Mesos无法和指定ID的slave通信。收到该消息的典型相应是将之前在slave上运行的任务调度到另一个新的slave上重新执行
    public void slaveLost(SchedulerDriver schedulerDriver, Protos.SlaveID slaveID) { }

    // 通知调度器特定的执行器已经以指定状态结束了
    public void executorLost(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, int i) {}

    // 在一个可恢复的错误产生或者调度器和驱动停止工作时被调用，该方法应该清理所有使用的资源
    public void error(SchedulerDriver schedulerDriver, String message) {
        System.err.println("Error : " + message);
    }

}
