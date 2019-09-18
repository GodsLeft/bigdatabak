import java.lang.reflect.*;
import java.lang.annotation.*;
@SuppressWarnings(value="unchecked")
@Deprecated
public class ClassTest{
    private ClassTest(){}
    public ClassTest(String name){
        System.out.println("执行有参数的构造器");
    }
    public void info(){
        System.out.println("执行无参数的info方法");
    }
    public void info(String str){
        System.out.println("执行有参数的info方法"+",其str参数值："+str);
    }

    class Inner{}

    public static void main(String[] args) throws Exception{
        Class<ClassTest> clazz = ClassTest.class;

        System.out.println("=============================================");
        Constructor[] ctors = clazz.getDeclaredConstructors();
        System.out.println("classTest的全部构造器如下：");
        for(Constructor c:ctors){
            System.out.println(c);
        }

        System.out.println("=============================================");
        Constructor[] publicCtors = clazz.getConstructors();
        System.out.println("ClassTest的全部public构造器如下：");
        for(Constructor c:publicCtors){
            System.out.println(c);
        }

        System.out.println("=============================================");
        Method[] mtds = clazz.getMethods();
        System.out.println("ClassTest的全部public方法如下：");
        for(Method md:mtds){
            System.out.println(md);
        }

        System.out.println("=============================================");
        System.out.println("ClassTest带一个字符串参数的info方法为："+clazz.getMethod("info",String.class));

        Annotation[] ans = clazz.getAnnotations();
        System.out.println("ClassTest的全部annotation为：");
        for(Annotation an:ans){
            System.out.println(an);
        }
        System.out.println("该元素上的@SuppressWarnings注释为："+clazz.getAnnotation(SuppressWarnings.class));

        System.out.println("=============================================");
        Class<?>[] inners = clazz.getDeclaredClasses();
        System.out.println("ClassTest的全部内部类如下：");
        for(Class c : inners){
            System.out.println(c);
        }

        System.out.println("=============================================");

        Class inClazz = Class.forName("ClassTest$Inner");
        System.out.println("inClazz对应的外部类为："+inClazz.getDeclaringClass());
        System.out.println("ClassTest的包为："+clazz.getPackage());
        System.out.println("ClassTest的父类："+clazz.getSuperclass());
    }
}
