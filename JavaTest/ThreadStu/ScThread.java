class Account{
    private String accountNo;
    private double balance;
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
    public void setBalance(double balance){
        this.balance = balance;
    }
    public double getBalance(){
        return balance;
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
    public DrawThread(String name,Account account,double drawAmount){
        super(name);
        this.account = account;
        this.drawAmount = drawAmount;
    }
    public void run(){
        //同步块、也可以修饰可变类的方法
        synchronized(account){
            if(account.getBalance() >= drawAmount){
                System.out.println(getName()+"成功"+drawAmount);
                try{
                    Thread.sleep(1);
                }catch(InterruptedException ex){
                    ex.printStackTrace();
                }
                account.setBalance(account.getBalance() - drawAmount);
                System.out.println("\t余额："+account.getBalance());
            }else{
                System.out.println(getName()+"取钱失败");
            }
        }
    }
}
public class ScThread{
    public static void main(String[] args){
         Account acct = new Account("123456",1000);
         new DrawThread("甲",acct,800).start();
         new DrawThread("乙",acct,800).start();
    }
}
