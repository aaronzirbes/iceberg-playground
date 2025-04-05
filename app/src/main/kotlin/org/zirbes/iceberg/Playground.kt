package org.zirbes.iceberg

import java.time.LocalDate
import org.zirbes.iceberg.config.RepositoryConfig
import org.zirbes.iceberg.dao.BucketInitializer
import org.zirbes.iceberg.dao.ProjectTable
import org.zirbes.iceberg.model.Priority

class Playground {

    private val config = RepositoryConfig()

    fun run() {
        println("🏃Running.")

        BucketInitializer(config.creds).bootstrap(config.bucket)
        val repository = ProjectTable(config)
        println("🪣Repository Initialized.")

        val retrievedProject = repository.get(1)
        println("🏠Retrieved project: $retrievedProject")

        val allProjects = repository.list()
        println("🏠All projects: $allProjects")

        val lowPriorityProjects = repository.findPriorityLessThan(Priority.MEDIUM)
        println("🏠Low priority projects: $lowPriorityProjects")

        val highPriorityProjects = repository.findPriorityGreaterThan(Priority.MEDIUM)
        println("🏠High priority projects: $highPriorityProjects")

        val overdueProjects = repository.findDueBefore(LocalDate.now())
        println("🏠Overdue projects: $overdueProjects")

    }
}