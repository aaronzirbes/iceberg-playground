package org.zirbes.iceberg.dao

import org.apache.iceberg.Schema
import org.apache.iceberg.types.Types

val PROEJCT_SCHEMA = Schema(
    Types.NestedField.required(1, "id", Types.IntegerType.get()),
    Types.NestedField.required(2, "name", Types.StringType.get()),
    Types.NestedField.optional(3, "description", Types.StringType.get()),
    Types.NestedField.required(4, "priority", Types.IntegerType.get()),
    Types.NestedField.optional(5, "due", Types.DateType.get())
)
