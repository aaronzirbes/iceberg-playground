package org.zirbes.iceberg

import java.time.LocalDate
import org.zirbes.iceberg.config.RepositoryConfig
import org.zirbes.iceberg.dao.ProjectRepository
import org.zirbes.iceberg.model.Priority

class Playground {

    val config = RepositoryConfig()

    fun run() {
        println("🏃Running.")

        val repository = ProjectRepository(config)
        println("🪣Repository Initialized.")

        val retrievedProject = repository.get(1)
        println("🏠Retrieved project: $retrievedProject")

        val allProjects = repository.list()
        println("🏠All projects: $allProjects")

        val lowPriorityProjects = repository.findPriorityLessThan(Priority.MEDIUM.intValue)
        println("🏠Low priority projects: $lowPriorityProjects")

        val highPriorityProjects = repository.findPriorityGreaterThan(Priority.MEDIUM.intValue)
        println("🏠High priority projects: $highPriorityProjects")

        val overdueProjects = repository.findDueBefore(LocalDate.now())
        println("🏠Overdue projects: $overdueProjects")

    }
}