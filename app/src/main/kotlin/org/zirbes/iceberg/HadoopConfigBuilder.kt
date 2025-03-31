package org.zirbes.iceberg

import org.apache.hadoop.conf.Configuration

class HadoopConfigBuilder(creds: ObjectStorageCreds) {


    val build: Configuration = Configuration().apply {
        set("fs.s3a.endpoint", creds.endpoint) // or your MinIO server
        set("fs.s3a.access.key", creds.accessKey)
        set("fs.s3a.secret.key", creds.secretKey)
        set("fs.s3a.path.style.access", creds.pathStyleAccess.toString()) // important for MinIO
        set("fs.s3a.connection.ssl.enabled", creds.sslEnabled.toString()) // MinIO often runs without SSL
        set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    }

}