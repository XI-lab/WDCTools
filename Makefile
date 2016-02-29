assembly:
	sbt assembly
run:
	spark-submit \
	--master yarn-master \
	--conf spark.ui.port=$(shuf -i 2000-65000 -n 1) \
	--class "feedsTransform" \
	target/scala-2.10/feedTransform-assembly-1.0.jar
