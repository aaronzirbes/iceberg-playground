package org.zirbes.iceberg.model

import java.time.LocalDate
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory

data class Project(
    val id: Int,
    val name: String,
    val description: String,
    val priority: Priority,
    val due: LocalDate,
) {

    fun toRow(): Row =
        RowFactory.create(
            id,
            name,
            description,
            priority.intValue,
            due
        )
}

enum class Priority(i: Int) {
    LOW(10),
    MEDIUM(50),
    HIGH(90);

    val intValue: Int = i

    companion object {
        fun fromValue(value: Int): Priority =
            entries.firstOrNull { it.intValue == value } ?: LOW
    }
}
