package org.zirbes.iceberg

data class ObjectStorageCreds(
    val endpoint: String,
    val accessKey: String,
    val secretKey: String,
    val pathStyleAccess: Boolean = true,
    val sslEnabled: Boolean = false,
) {
    companion object {
        val LOCAL = ObjectStorageCreds(
            endpoint = "http://127.0.0.1:9006",
            accessKey = "iceberg",
            secretKey = "playground",
            pathStyleAccess = true,
            sslEnabled = false
        )
    }
}
