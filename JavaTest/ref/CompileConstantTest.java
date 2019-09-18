class MyTest{
    static{
        System.out.println("静态初始化块。。。");
    }
    static final String compileConstant = "hello world";
    static String con = "zhu hello";
}

public class CompileConstantTest{
    public static void main(String[] args){
        System.out.println(MyTest.compileConstant);
        System.out.println(MyTest.con);
    }
}
