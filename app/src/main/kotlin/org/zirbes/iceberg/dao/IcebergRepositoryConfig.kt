package org.zirbes.iceberg.dao

import org.apache.iceberg.Schema
import org.zirbes.iceberg.ObjectStorageCreds

data class IcebergRepositoryConfig(
    val creds: ObjectStorageCreds = ObjectStorageCreds.LOCAL,
    val location: String = "s3a://my-iceberg-bucket/projects",
    val schema: Schema = PROEJCT_SCHEMA

)