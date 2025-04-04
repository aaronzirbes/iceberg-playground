package org.zirbes.iceberg.config

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.hadoop.HadoopCatalog

class HadoopCatalogBuilder(config: RepositoryConfig) {

    private val location = config.location

    internal val settings = mapOf(
        "fs.s3a.endpoint" to config.creds.endpoint,
        "fs.s3a.access.key" to config.creds.accessKey,
        "fs.s3a.secret.key" to config.creds.secretKey,
        "fs.s3a.path.style.access" to config.creds.pathStyleAccess.toString(),
        "fs.s3a.connection.ssl.enabled" to config.creds.sslEnabled.toString(),
        "fs.s3a.impl" to "org.apache.hadoop.fs.s3a.S3AFileSystem",
    )

    private val hadoopConfig : Configuration = Configuration().apply {
        settings.forEach(::set)
    }

    fun build() = HadoopCatalog(hadoopConfig, location)

}