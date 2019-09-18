spark-submit \
--class "SimplePageRank" \
--master spark://master:7077 \
target/scala-2.10/simple-pagerank_2.10-1.0.jar
