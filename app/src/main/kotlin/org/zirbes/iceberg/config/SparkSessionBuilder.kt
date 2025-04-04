package org.zirbes.iceberg.config

import org.apache.spark.sql.SparkSession

class SparkSessionBuilder(
    config: RepositoryConfig,
    hadoopConfigBuilder: HadoopCatalogBuilder = HadoopCatalogBuilder(config)
) {

    private val catalogName = config.catalog
    private val warehouseName = config.warehouse

    private val sparkSession: SparkSession.Builder = SparkSession.builder()
        .appName(config.app)
        .master("local[*]")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.${catalogName}", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.${catalogName}.type", "hadoop")
        .config("spark.sql.catalog.${catalogName}.${warehouseName}", config.location)

    init {
        hadoopConfigBuilder.settings.forEach(sparkSession::setHadoop)
    }

    fun build(): SparkSession = sparkSession.orCreate

}

