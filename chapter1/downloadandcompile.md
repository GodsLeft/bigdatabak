# 下载编译
## 环境准备
- git
- sbt
- maven

## 下载
- 先将在github上fork spark源代码

```bash
git clone git@github.com:GodsLeft/spark.git
```

## 编译
```bash
export MAVEN_OPTS="-Xmx4g -XX:ReservedCodeCacheSize=512m"

cd /home/left/spark
export SPARK_PREPEND_CLASSES=true
sbt compile package

#因为不能使用R所以不要使用-Psparkr选项
#../dev/make-distribution.sh --name zhu-spark --tgz -Psparkr -Phadoop-2.7 -Dhadoop.version=2.7.2 -Phive -Phive-thriftserver -Pyarn

# 不指定scala的版本也是可以打包成功的,这个命令最好放置在spark源码根目录下执行
#./dev/make-distribution.sh --name zhu-spark --tgz  -Phadoop-2.7 -Dhadoop.version=2.7.2 -Dscala-2.10.6 -Phive -Phive-thriftserver -Pyarn
time ./dev/make-distribution.sh --name zhu-spark --tgz  -Phadoop-2.7 -Dhadoop.version=2.7.2 -Phive -Phive-thriftserver -Pyarn

time ./build/mvn -Pyarn -Phadoop-2.7 -Dhadoop.version=2.7.2 -DskipTests clean package
```
