### 
- 系统中的所有类实际上也是实例，都是java.lang.Class的实例


### File
- CompileConstantTest:类初始化的时机
- ClassLoaderTest:类在加载的时候并不会进行初始化
- BootstrapTest.java:
- CompileClassLoader.java:自定的类加载器
- **ClassTest.java**:简单的、很重要的基础反射
- **ObjectPoolFactory.java**:简单对象池,使用的是默认的构造器

### 获得Class对象
- Class.forName(String clazzName):
- Person.class:返回Person类对应的Class对象
- 调用某个对象的getClass()方法：返回该对象的所属类对应的Class对象
- 
