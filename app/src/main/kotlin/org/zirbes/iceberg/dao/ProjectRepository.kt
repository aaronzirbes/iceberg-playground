package org.zirbes.iceberg.dao

import java.time.LocalDate
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Table
import org.apache.iceberg.hadoop.HadoopTables
import org.zirbes.iceberg.HadoopConfigBuilder
import org.zirbes.iceberg.Project

class ProjectRepository(config: IcebergRepositoryConfig) {

    private val schema = config.schema
    private val location = config.location
    private val tables = HadoopTables(HadoopConfigBuilder(config.creds).build)

    private val table: Table by lazy {
        println("🪣Creating table: $location.")
        tables.create(schema, PartitionSpec.unpartitioned(), location)
    }

    fun create(project: Project): Project = TODO()

    fun get(projectId: Int): Project = TODO()

    fun list(): List<Project> = TODO()

    fun findPriorityLessThan(priority: Int): List<Project> = TODO()
    fun findDueBefore(due: LocalDate): List<Project> = TODO()

    fun delete(projectId: Int): List<Project> = TODO()
    fun findPriorityGreaterThan(intValue: Int): List<Project> = TODO()

}