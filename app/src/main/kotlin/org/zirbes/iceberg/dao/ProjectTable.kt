package org.zirbes.iceberg.dao

import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Schema
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.types.Types
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.zirbes.iceberg.model.Project

class ProjectTable(
    private val catalog: HadoopCatalog,
    namespace: String = "default",
) {

    private val tableIdentifier = TableIdentifier.of(namespace, TABLE_NAME)
    private val sparkTableName = "${catalog}.${namespace}.${TABLE_NAME}"

    init {
        createIfNotExists()
    }

    fun write(spark: SparkSession, projects: Collection<Project>) {
        val dataFrame = createDataFrame(spark, projects)
        dataFrame.writeTo(sparkTableName).append()
    }

    fun list(spark: SparkSession): List<Project> {
        val results: Dataset<Row> = spark.sql(
            "SELECT * FROM $sparkTableName"
        )
        return results.collectAsList().map(Row::toProject)
    }

    fun get(spark: SparkSession, id: Int): Project? {
        val results: Dataset<Row> = spark.sql(
            "SELECT * FROM $sparkTableName WHERE id = $id"
        )
        return results.collectAsList()
            .map(Row::toProject)
            .firstOrNull()
    }

    fun createDataFrame(spark: SparkSession,
                        projects: Collection<Project>): Dataset<Row> {
        val rows: List<Row> = projects.map(Project::toRow)
        return spark.createDataFrame(rows, PROJECT_STRUCT)
    }

    private fun createIfNotExists() =
        if (catalog.tableExists(tableIdentifier)) {
            println("ℹ️ Table already exists.")
        } else {
            println("⚠️ Creating table.")
            catalog.createTable(tableIdentifier, PROJECT_SCHEMA, PartitionSpec.unpartitioned(), TABLE_PROPERTIES)
            println("ℹ️ Table created.")

        }

    companion object {

        private val TABLE_NAME = "projects"

        val PROJECT_SCHEMA = Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "description", Types.StringType.get()),
            Types.NestedField.required(4, "priority", Types.IntegerType.get()),
            Types.NestedField.optional(5, "due", Types.DateType.get())
        )

        val PROJECT_STRUCT = PROJECT_SCHEMA.toSparkStructType()

        val TABLE_PROPERTIES = mapOf(
            // "format-version" to "2",
            "write.format.default" to "parquet",
            "write.parquet.compression-codec" to "snappy",
            // "read.parquet.enable_vectorized_reader" to "true"
        )
    }
}
