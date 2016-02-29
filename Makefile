assembly:
	sbt assembly

transform:
	spark-submit \
	--master yarn-master \
	--deploy-mode client \
	--num-executors 20 \
	--executor-cores 2 \
	--driver-memory 4g \
	--executor-memory 3g \
	--conf spark.yarn.executor.memoryOverhead=1024 \
	--conf spark.ui.port=13338 \
	--class "info.exascale.feedTransform.feedsTransform" \
	target/scala-2.10/feedTransform-assembly-1.0.jar

coalesce:
	spark-submit \
	--master yarn-master \
	--deploy-mode client \
	--num-executors 20 \
	--executor-cores 2 \
	--driver-memory 4g \
	--executor-memory 3g \
	--conf spark.yarn.executor.memoryOverhead=1024 \
	--conf spark.ui.port=13338 \
	--class "info.exascale.feedTransform.coalesce" \
	target/scala-2.10/feedTransform-assembly-1.0.jar