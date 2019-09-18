package com.left;

import com.google.protobuf.ByteString;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;

/**
 * Created by m2015 on 2017/6/5.
 */
public class MonteCarloExecutor implements Executor {
    Expression expression;
    double xLow;
    double xHigh;
    double yLow;
    double yHigh;
    int n;

    public MonteCarloExecutor(Expression expression, double xLow, double xHigh, double yLow, double yHigh, int n) {
        this.expression = expression;
        this.xLow = xLow;
        this.xHigh = xHigh;
        this.yLow = yLow;
        this.yHigh = yHigh;
        this.n = n;
    }

    // 执行器驱动成功和Mesos连接之后调用，调度器可以用executorInfo中的data域将任意数据传递给执行器
    // FrameworkInfo包含了框架相关信息
    // slaveInfo:包含了将要运行执行器的slave
    public void registered(ExecutorDriver executorDriver, Protos.ExecutorInfo executorInfo, Protos.FrameworkInfo frameworkInfo, Protos.SlaveInfo slaveInfo) {
        System.out.println("Registered and executor on slave " + slaveInfo.getHostname());
    }

    // 执行器向重启的slave重新注册时调用
    public void reregistered(ExecutorDriver executorDriver, Protos.SlaveInfo slaveInfo) {
        System.out.println("Re-Registered an executor on slave " + slaveInfo.getHostname());
    }

    // 执行器失去和slave的连接
    public void disconnected(ExecutorDriver executorDriver) {
        System.out.println("Re-Disconnected the executor on slave");
    }

    // 当任务在当前执行器上启动时，被调用
    // 该方法调用时也会阻塞
    public void launchTask(final ExecutorDriver executorDriver, final Protos.TaskInfo taskInfo) {
        System.out.println("Launching task " + taskInfo.getTaskId().getValue());
        Thread thread = new Thread() {
            @Override
            public void run() {
                Protos.TaskStatus status = Protos.TaskStatus.newBuilder()
                        .setTaskId(taskInfo.getTaskId())
                        .setState(Protos.TaskState.TASK_RUNNING)
                        .build();
                executorDriver.sendStatusUpdate(status);
                System.out.println("Running task " + taskInfo.getTaskId().getValue());


                // 计算面积
                double pointsUnderCurve = 0;
                double totalPoints = 0;

                for(double x=xLow; x<xHigh; x+=(xHigh-xLow)/n) {
                    for(double y=yLow; y<yHigh; y+=(yHigh-yLow)/n) {
                        double value = expression.evaluate(x);
                        if(value >= y){
                            pointsUnderCurve ++;
                        }
                        totalPoints++;
                    }
                }
                double area = (xHigh - xLow)*(yHigh - yLow) * pointsUnderCurve/totalPoints;

                // 更新状态
                status = Protos.TaskStatus.newBuilder()
                        .setTaskId(taskInfo.getTaskId())
                        .setState(Protos.TaskState.TASK_FINISHED)
                        .setData(ByteString.copyFrom(Double.toString(area).getBytes()))
                        .build();
                executorDriver.sendStatusUpdate(status);
                System.out.println("Finished task " + taskInfo.getTaskId().getValue() + " with area : " + area);
            }
        };
        thread.start();
    }

    // 在当前执行器中任务被杀死时调用
    public void killTask(ExecutorDriver executorDriver, Protos.TaskID taskID) {
        System.out.println("Killing task " + taskID);
    }

    // 在框架消息到达执行器时被调用
    public void frameworkMessage(ExecutorDriver executorDriver, byte[] bytes){}
    public void shutdown(ExecutorDriver executorDriver) {
        System.out.println("Shutting down the executor");
    }
    public void error(ExecutorDriver executorDriver, String s){}

    public static void main(String[] args) {
        if(args.length < 6) {
            System.out.println("Usage: MonteCarloExecutor <Expression> <xLow> <xHigh> <yLow> <yHigh> <number of Points>");
        }
        MesosExecutorDriver driver = new MesosExecutorDriver(new MonteCarloExecutor(Expression.fromString(args[0]), Double.parseDouble(args[1]), Double.parseDouble(args[2]), Double.parseDouble(args[3]), Double.parseDouble(args[4]), Integer.parseInt(args[5])));
        Protos.Status status = driver.run();
        System.out.println("Driver exited with status " + status);
    }
}
