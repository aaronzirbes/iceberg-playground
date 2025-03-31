package org.zirbes.iceberg

import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.shouldBe
import java.time.LocalDate
import org.zirbes.iceberg.dao.IcebergRepositoryConfig
import org.zirbes.iceberg.dao.ProjectRepository

class PlaygroundSpec : ShouldSpec({

    val config = IcebergRepositoryConfig()

    val repository = ProjectRepository(config)

    val entry = Project(
        id = 1,
        name = "Coffered ceiling",
        description = "Entryway coffered ceiling with stained glass accent",
        priority = Priority.LOW,
        due = LocalDate.parse("2025-05-01")
    )
    val closet = Project(
        id = 2,
        name = "Upstairs Hallway Project",
        description = "Repair plaster, patch and paint upstairs hallway closet",
        priority = Priority.MEDIUM,
        due = LocalDate.parse("2025-06-01")
    )

    should("be able to create projects") {
        repository.create(entry)
        println("Project created: $entry")
        repository.create(closet)
        println("Project created: $closet")
    }

    should("be able to retrieve projects") {
        val moveInDate = LocalDate.parse("2025-05-15")
        val retrievedProject = repository.get(entry.id)
        println("Retrieved project: $retrievedProject")

        val allProjects = repository.list()
        println("All projects: $allProjects")

        allProjects.size shouldBe 2

        val lowPriorityProjects = repository.findPriorityLessThan(2)
        println("Low priority projects: $lowPriorityProjects")

        lowPriorityProjects.size shouldBe 1

        val upcomingProjects = repository.findDueBefore(moveInDate)
        println("Upcoming due projects: $upcomingProjects")
        upcomingProjects.size shouldBe 1
    }

    should("be able to delete projects") {
        repository.delete(entry.id)

        val remaining = repository.list()

        remaining.size shouldBe 1
    }

})
