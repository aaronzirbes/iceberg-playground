package org.zirbes.iceberg.dao

import java.time.LocalDate
import org.apache.iceberg.hadoop.HadoopCatalog
import org.zirbes.iceberg.config.HadoopCatalogBuilder
import org.zirbes.iceberg.config.RepositoryConfig
import org.zirbes.iceberg.config.SparkSessionBuilder
import org.zirbes.iceberg.model.Project

class ProjectRepository(config: RepositoryConfig) {

    private val catalog: HadoopCatalog = HadoopCatalogBuilder(config).build()
    private val spark = SparkSessionBuilder(config).build()
    private val projectTable = ProjectTable(catalog)

    fun create(project: Project) = projectTable.write(spark, listOf(project))
    fun create(projects: Collection<Project>) = projectTable.write(spark, projects)
    fun list(): List<Project>  = projectTable.list(spark)
    fun get(projectId: Int): Project? = projectTable.get(spark, projectId)

    fun delete(projectId: Int): List<Project> = TODO()
    fun findPriorityLessThan(priority: Int): List<Project> = TODO()
    fun findDueBefore(due: LocalDate): List<Project> = TODO()
    fun findPriorityGreaterThan(intValue: Int): List<Project> = TODO()

}