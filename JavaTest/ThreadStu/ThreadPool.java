import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
class Thd implements Runnable{
    public void run(){
        for(int i=0;i<10;i++){
            System.out.println(Thread.currentThread().getName()+"-->"+i);
        }
    }
}
public class ThreadPool{
    public static void main(String[] args) throws Exception{
        ExecutorService pool = Executors.newFixedThreadPool(6);
        pool.submit(new Thd());
        pool.submit(new Thd());
        pool.shutdown();
    }
}
