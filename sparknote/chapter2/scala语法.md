# scala常用数据类型
- 八进制：0开头
- 浮点数之后有f或者F后缀时，表示是一个Float类型，否则是一个Double类型

# scala变量
- var声明变量：var myvar:String = "Java"
- val声明常量：val myval:String = "Hello"
- 可以不指明数据类型：类型推断，但是必须赋初始值
- val a,b = 9
- val (a: Int, b: String) = Pair(9, "Hello world"):返回元组的函数

# scala访问修饰符
- 默认public
- 保护成员只能在子类中被访问，比java严格，java可以在同一个包中被访问
- 作用域保护：private[packageName]

# scala循环
- yield生成
```scala
//break要引入该包
import scala.util.control._
var loop = new Breaks
loop.breakable{
    for(n <- list){
        println(n)
        if(n == 10){
            loop.break()
        }
    }
}
```

# scala函数
- `def functionName([参数列表]):[return type] = {}`
- 如果不写等号和方法体，方法将会被隐式声明为抽象

# scala闭包
- 闭包是一个函数，返回值依赖于在函数外部的一个多个变量

# scala字符串
```scala
var sb = new StringBuilder
sb += 'a'
sb ++= 'cdefg'
```

# scala多维数组
```scala
var arr = Array.ofDim[Int](2,3)
var arr = new Array[Array[Int]](5)
val arr = range(10, 20, 2  )
```

# scala集合
- List
- Set
- Map
- 元组
- Option：有可能包含值，也有可能不包含值的容器，可以看做一个0到1一个元素的容器
- Iterator

# scala类与对象
- scala中没有static，但是提供了object
- object对象不能够带参数
- 单例对象与某个类共享同一个名称时，被称为这个类的伴生对象
- 类和它的伴生对象可以互相访问其私有成员

# scala特征
- 相当于Java的接口，与接口不同的是可以定义属性和方法的实现
- scala只能继承单一父类，但是Trait的话就可以继承多个

# 模式匹配
```scala
def matchTest(x:Any):Any = x match{
    case 1 => "scala"
    case "java" => 2
    case y:String => "php"
    case _ => "other"
}
```

# 样例类
- 是特殊的类，经过优化用来进行模式匹配
```scala
case class Course(name:String, cid:Int)
```

# 正则表达式
```scala
val regex = "([0-9]+)\\s+([a-zA-Z]+)".r
val line = "520  lishinihaome"
val regex(num, str) = line              //这里的val声明的是num,str
println(num+ ":" + str)

val list = regex.findAllIn(line)
list.foreach(println)
```

# scala异常处理
- throw
```scala
try{

}catch{
    case ex:Exception => println("exception")
    case ex:IllegalArgumentException => println("parameter is not Integer")
}finally{
    println("finally")
}
```

# scala提取器
- 从传递给它的对象中提取出构造该对象的参数
- 是带有unapply方法的对象
- apply unapply
- 提取器模式匹配
```scala
object Test{
    def main(args:Array[String]):Unit = {
        val x = Test(9)
        println(x)

        x match {
            case Test(num) => println(num) //num是多少？
            case _ => println("nothing")
        }
    }
    def apply(x:Double) = x*x
    def unapply(x:Double):Option[Double] = Some(math.sqrt(x))
}
```

# scala文件IO
- 读写文件仍然用的是java的方法java.io
- Source.fromFile("").foreach(print)

# scala柯里化
- 将原来接收两个参数的函数变成新的接收一个参数的函数的过程
- 新函数返回一个以原有第二个参数为参数的函数
- (x, y) => (x)(y)类似过程
```scala
object Hello{
    def main(args:Array[String]):Unit = {

    }
    def multifyBy(factor:Double) = (x:Double) => factor * x
    def multifyBy(factor:Double)(x:Double) = factor * x //柯里化
    def multifyBy(factor:Double, x:Double) = factor * x
}
```

# scala高阶函数
- 将一个函数当做另外一个函数的参数
- 返回值是函数的函数

## 常用高阶函数
- map
- flatMap
- filter
- reduce
- fold
- scan

```scala
//fold
Array(1,2,3,4,5).foldLeft(0)((x:Int, y:Int) => println(x,y); x+y)
Array(1,2,3,4,5).foldRight(0)((x:Int, y:Int) => println(x, y); x+y)
```

# scala trait
- 在trait中可以有函数的实现，这点和java不同
- 如果在trait中有实现的时候，在子类中必须使用override关键字

# 包对象
- scala默认引用了几个包
    + import java.lang._
    + import scala._
    + import Predef._
- import java.awt.{Color,Font}
- import java.util.{HashMap => JavaHashMap}：避免命名冲突
- import scala.{StringBuilder => _}:隐藏

# case class object
- 传递消息
- 模式匹配
- 自动带有伴生对象
```scala
abstract class Person
case class Student(age: Int) extends Person //age并没有指定var 或者val 默认val 不可变
case class Worker(age: Int, salary: Double) extends Person
```

# 类型参数
```scala
[T <: Comparable[T]]    //类型上界，T必须是Comparable的子类型
[T >: T]                //类型下界
[T <% Comparable[T]]    //视图界定，意味着T可以被隐式转换成Comparable[T]
[T : M]                 //上下文界定，必须存在一个类型为Ordering[T]的隐式值

//类型约束
[T =:= U]               //测试T是否等于U
[T <:< U]               //测试T是否是U的子类型
[T <%< U]               //测试T是否能隐式的转换为U的子类型

```

# Option
- sealed关键字：要求定义Option class object 都必须在同一个文件中
```scala
sealed abstract class Option[+A] extends Product with Serializable{}

//
package com.left;
object Option_Internal{
    def main(args: Array[String]){
        val scores = Map("Alice"->99, "Spark"->100)
        scores.get("Alice") match {
            case Some(score) => println(score)
            case None => println("No score")
        }
    }
}
```

# List
```scala
val bigdata_core = "Hadoop" :: ("spark" :: Nil)
```



# Actor
- 每个Actor都有自己的邮箱