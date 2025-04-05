package org.zirbes.iceberg.dao

import io.minio.MakeBucketArgs
import io.minio.MinioClient
import org.zirbes.iceberg.config.ObjectStoreConfig

class BucketInitializer(
    creds: ObjectStoreConfig = ObjectStoreConfig.LOCAL,
) {


    val client: MinioClient by lazy {
        MinioClient
            .builder()
            .region("us")
            .endpoint(creds.endpoint)
            .credentials(creds.accessKey, creds.secretKey)
            .build()
    }

    fun bootstrap(bucket: String) {
        val allBuckets = client.listBuckets()
        val bucketExists = allBuckets.any { it.name() == bucket }
        if (bucketExists) {
            println("🪣 Found bucket: $bucket.")
        } else {
            println("🪣 Making bucket: $bucket.")
            client.makeBucket(MakeBucketArgs.builder().bucket(bucket).build())
        }

    }
}