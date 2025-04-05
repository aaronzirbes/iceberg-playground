package org.zirbes.iceberg.model

import java.time.LocalDate

data class Project(
    val id: Int,
    val name: String,
    val description: String?,
    val priority: Priority,
    val due: LocalDate?,
)

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
