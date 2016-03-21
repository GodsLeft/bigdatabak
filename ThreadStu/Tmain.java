import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
class Thd extends Thread{
    private static int i;
    public void run(){
        for(;i<4;i++){
            System.out.println(getName()+" "+i);
        }
    }
}
class Thdd implements Runnable{
    private int i;
    public void run(){
        for(;i<4;i++){
            System.out.println(Thread.currentThread().getName()+" "+i);
        }
    }
}

class Thddd implements Callable<Integer>{
    private int i;
    public Integer call(){
        for(;i<4;i++){
            System.out.println(Thread.currentThread().getName()+" "+i);
        }
        return i;
    }
}

//后台进程
class DaemonThread extends Thread{
    public void run(){
        for(int i = 0;i<1500;i++){
            System.out.println(getName()+" "+i);
        }
    }
}

class Yield extends Thread{
    public Yield(String name){
        super(name);
    }
    public void run(){
        for(int i=0;i<20;i++){
            System.out.println(getName()+" "+i);
            if(i==10){
                Thread.yield();
            }
        }
    }
}
public class Tmain{
    public static void main(String[] args) throws Exception{
/*
        new Thd().start();
        new Thd().start();
/*        Thdd st = new Thdd();
        new Thread(st,"thdd1").start();
        new Thread(st,"thdd2").start();
*/
/*
        Thddd rt = new Thddd();
        FutureTask<Integer> task = new FutureTask<Integer>(rt);
        Thread a = new Thread(task,"thddd1");
        a.start();
        new Thread(task,"thddd2").start();
        //a.start();
        System.out.println(task.get());
        System.out.println(a.isAlive());
*/
/*
        DaemonThread t = new DaemonThread();
        t.setDaemon(true);
        t.start();
        for(int i=0;i<10;i++){
             System.out.println(Thread.currentThread().getName()+" "+i);

        }
*/
        Yield yt1 = new Yield("高级");
        yt1.setPriority(Thread.MAX_PRIORITY);
        yt1.start();
        Yield yt2 = new Yield("低级");
        yt2.setPriority(Thread.MIN_PRIORITY);
        yt2.start();
    }
}
