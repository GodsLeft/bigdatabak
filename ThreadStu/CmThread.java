class Account{
    private String accountNo;
    private double balance;
    private boolean flag = false;
    public Account(){}
    public Account(String accountNo,double balance){
        this.accountNo = accountNo;
        this.balance = balance;
    }
    public void setAccountNo(String accountNo){
        this.accountNo = accountNo;
    }
    public String getAccountNo(){
        return this.accountNo;
    }
    public double getBalance(){
        return this.balance;
    }
    //同步方法
    public synchronized void draw(double drawAmount){
        try{
            if(!flag){
                wait();//导致当前线程等待
            }else{
                System.out.println(Thread.currentThread().getName()+"取钱："+drawAmount);
                balance -= drawAmount;
                System.out.println("账户余额："+balance);
                flag = false;
                notifyAll();//唤醒在此同步监视器上等待的线程
            }
        }catch(InterruptedException ex){
            ex.printStackTrace();
        }
    }
    public synchronized void deposit(double depositAmount){
        try{
            if(flag){
                 wait();
            }else{
                System.out.println(Thread.currentThread().getName()+"存款："+depositAmount);
                balance += depositAmount;
                System.out.println("账户余额："+balance);
                flag = true;
                notifyAll();
            }
        }catch(InterruptedException ex){
             ex.printStackTrace();
        }
    }
    public int hashCode(){
        return accountNo.hashCode();
    }
    public boolean equals(Object obj){
        if(this == obj)
            return true;
        if(obj != null && obj.getClass() == Account.class){
             Account target = (Account)obj;
             return target.getAccountNo().equals(accountNo);
        }
        return false;
    }
}

class DrawThread extends Thread{
    private Account account;
    private double drawAmount;
    public DrawThread(String name, Account account, double drawAmount){
        super(name);
        this.account = account;
        this.drawAmount = drawAmount;
    }

    public void run(){
        for(int i =0;i<10;i++){
            account.draw(drawAmount);
        }
    }
}

class DepositThread extends Thread{
    private Account account;
    private double depositAmount;
    public DepositThread(String name,Account account,double depositAmount){
        super(name);
        this.account = account;
        this.depositAmount = depositAmount;
    }
    public void run(){
        for(int i=0;i<10;i++){
            account.deposit(depositAmount);
        }
    }
}
/*这个程序最终被阻塞
 * 取钱者只有100次 存钱却有300次
 */
public class CmThread{
    public static void main(String[] args){
        Account acct = new Account("123456",0);
        new DrawThread("取钱者",acct,800).start();
        new DepositThread("存款者甲",acct,800).start();
        new DepositThread("存款者乙",acct,800).start();
        new DepositThread("存款者丙",acct,800).start();
    }
}
