import java.lang.reflect.*;
public class Hello{
    public Hello(){}
    public Hello(String llo){
        System.out.println(llo);
    }
    public static void main(String[] args) throws Exception{
        Class<?> clazz = Class.forName("Hello");
        Constructor ctor = clazz.getConstructor(String.class);
        Object obj = ctor.newInstance("zhuyaguang");
        System.out.println(obj);
        for(String arg:args){
            System.out.println("运行Hello的参数："+arg);
        }
    }
}
