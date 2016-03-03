assembly:
	sbt assembly

feedtransform:
	echo done && exit && \
	spark-submit \
	--master yarn-master \
	--deploy-mode client \
	--num-executors 20 \
	--executor-cores 2 \
	--driver-memory 4g \
	--executor-memory 3g \
	--conf spark.yarn.executor.memoryOverhead=1024 \
	--conf spark.ui.port=13338 \
	--class "info.exascale.wdctools.feedsTransform" \
	target/scala-2.10/wdctools-assembly-1.0.jar

coalesce:
	echo done && exit && \
	spark-submit \
	--master yarn-master \
	--deploy-mode client \
	--num-executors 20 \
	--executor-cores 2 \
	--driver-memory 4g \
	--executor-memory 3g \
	--conf spark.yarn.executor.memoryOverhead=1024 \
	--conf spark.ui.port=13339 \
	--class "info.exascale.wdctools.coalesce" \
	target/scala-2.10/wdctools-assembly-1.0.jar

urls:
	echo done && exit && \
	spark-submit \
	--master yarn-master \
	--deploy-mode client \
	--num-executors 20 \
	--executor-cores 2 \
	--driver-memory 4g \
	--executor-memory 3g \
	--conf spark.yarn.executor.memoryOverhead=1024 \
	--conf spark.ui.port=13340 \
	--class "info.exascale.wdctools.urlsToParquetSnappy" \
	--packages com.databricks:spark-csv_2.10:1.3.0 \
	target/scala-2.10/wdctools-assembly-1.0.jar

urltransform:
	echo done && exit && \
	spark-submit \
	--master yarn-master \
	--deploy-mode client \
	--num-executors 20 \
	--executor-cores 2 \
	--driver-memory 4g \
	--executor-memory 3g \
	--conf spark.yarn.executor.memoryOverhead=1024 \
	--conf spark.ui.port=13341 \
	--class "info.exascale.wdctools.urlsTransform" \
	target/scala-2.10/wdctools-assembly-1.0.jar

anchortransform:
	spark-submit \
	--master yarn-master \
	--deploy-mode client \
	--num-executors 20 \
	--executor-cores 2 \
	--driver-memory 4g \
	--executor-memory 3g \
	--conf spark.yarn.executor.memoryOverhead=1024 \
	--conf spark.ui.port=13341 \
	--class "info.exascale.wdctools.anchorsTransform" \
	target/scala-2.10/wdctools-assembly-1.0.jar
