package org.zirbes.iceberg.config

data class RepositoryConfig(
    val app: String = "iceberg-playground",
    val creds: ObjectStoreConfig = ObjectStoreConfig.LOCAL,
    val bucket: String = "zorgberg-bucket",
    val warehouse: String = "sandbox_warehouse",
    val catalog: String = "sandbox_catalog",
    val namespace: String = "default"
) {
    val location: String = "s3a://$bucket/$warehouse"
}