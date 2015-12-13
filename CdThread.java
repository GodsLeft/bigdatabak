class Account{
    private final Lock lock = new ReentrantLock();
    private final Condition cond = lock.newCondition();
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

    public void draw(double drawAmount){
        lock.lock();
        try{
            if(!flag){
                cond.await();
            }else{
                System.out.println(Thread.currentThread().getName()+"取钱："+drawAmount);
                balance -= drawAmount;
                System.out.println("账户余额："+balance);
                flag = false;
                cond.signalAll();
            }
        }catch(InterruptedException ex){
            ex.printStackTrace();
        }finally{
            lock.unlock();
        }
    }

    public void deposit(double depositAmount){
        lock.lock();
        try{
            if(flag){
                cond.await();
            }else{
                System.out.println(Thread.currentThread().getName()+"存款："+depositAmount);
                balance += depositAmount;
                System.out.println("账户余额："+balance);
                flag = true;
                cond.signalAll();
            }
        }catch(InterruptedException ex){
            ex.printStackTrace();
        }finally{
            lock.unlock();
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
/*这里的Account使用了lock
 * 这里不再写存取的线程了
 */
public class CdThread{
    public static void main(String[] args){}
}
