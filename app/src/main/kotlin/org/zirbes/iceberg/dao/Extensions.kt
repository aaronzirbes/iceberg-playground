package org.zirbes.iceberg.dao

import org.apache.iceberg.Schema
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.types.Types
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.zirbes.iceberg.dao.ProjectTable.Companion.PROJECT_SCHEMA
import org.zirbes.iceberg.model.Project

fun Project.toRecord(): GenericRecord {
    val record = GenericRecord.create(PROJECT_SCHEMA)
    record.set(1, this.id)
    record.set(2, this.name)
    record.set(3, this.description)
    record.set(4, this.priority)
    record.set(5, this.due)

    return record
}

fun Types.NestedField.toSpark(): StructField {
    val sparkType: DataType = when (this.type()) {
        is Types.IntegerType -> DataTypes.IntegerType
        is Types.StringType -> DataTypes.StringType
        is Types.DateType -> DataTypes.DateType
        else -> throw IllegalArgumentException("Unsupported type: ${this.type()}")
    }
    return StructField(this.name(), sparkType, this.isOptional, Metadata())
}

fun Schema.toSparkStructType(): StructType {
    val fields = this.columns().map(Types.NestedField::toSpark)
    return StructType(fields.toTypedArray())
}

fun Row.toProject(): Project =
    Project(
        id = this.getAs("id"),
        name = this.getAs("name"),
        description = this.getAs("description"),
        priority = this.getAs("priority"),
        due = this.getAs("due")
    )
