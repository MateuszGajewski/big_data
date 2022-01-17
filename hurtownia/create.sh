cat drop_tables.scala | spark-shell --packages io.delta:delta-core_2.12:0.8.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" -i
spark-submit --master yarn --num-executors 5 --driver-memory 512m --executor-memory 512m --executor-cores 1 --packages io.delta:delta-core_2.12:0.8.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtensi
on" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" czasETL.jar project
spark-submit --master yarn --num-executors 5 --driver-memory 512m --executor-memory 512m --executor-cores 1 --packages io.delta:delta-core_2.12:0.8.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtensi
on" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" kategoriaDrogiETL.jar project
spark-submit --master yarn --num-executors 5 --driver-memory 512m --executor-memory 512m --executor-cores 1 --packages io.delta:delta-core_2.12:0.8.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtensi
on" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" pogodaETL.jar project
spark-submit --master yarn --num-executors 5 --driver-memory 512m --executor-memory 512m --executor-cores 1 --packages io.delta:delta-core_2.12:0.8.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtensi
on" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" przestrzenETL.jar project
spark-submit --master yarn --num-executors 5 --driver-memory 512m --executor-memory 512m --executor-cores 1 --packages io.delta:delta-core_2.12:0.8.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtensi
on" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" typPojazduETL.jar project
# spark-submit --master yarn --num-executors 5 --driver-memory 512m --executor-memory 512m --executor-cores 1 --packages io.delta:delta-core_2.12:0.8.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtensi
# on" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" faktyETL.jar project
