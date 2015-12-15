# Run on a Spark standalone cluster
./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master spark://207.184.161.138:7077 \
  --executor-memory 20G \
  --total-executor-cores 100 \
  /path/to/examples.jar \
  1000





  spark-submit --class scala.streaming.NetworkWordCount --master spark://localhost:7077 /Users/wangdabin1216/IdeaProjects/sbtSpark/target/scala-2.10/sbtspark_2.10-1.0.jar localhost 9999


