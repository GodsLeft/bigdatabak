# 说明
- 这是一个简单的Mesos上的框架
- 计算曲线与坐标轴围成图形的面积

# 使用方法
```bash
mvn package
docker-compose up -d
docker exec montecarloarea_mmaster_1 bash -c "java -cp /tmp/bin/MonteCarloArea*.jar com.left.App 4 x 0 10 0 10 1000"
docker-compose stop
```

# 其他
- https://github.com/dharmeshkakadia/MonteCarloArea
- 执行命令时找不到 com.left.App类，怀疑和docker volume挂载有关
