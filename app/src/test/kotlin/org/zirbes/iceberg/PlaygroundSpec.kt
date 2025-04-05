package org.zirbes.iceberg

import io.kotest.assertions.assertSoftly
import io.kotest.core.spec.style.ShouldSpec
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import java.time.LocalDate
import org.zirbes.iceberg.config.RepositoryConfig
import org.zirbes.iceberg.dao.BucketInitializer
import org.zirbes.iceberg.dao.ProjectTable
import org.zirbes.iceberg.model.Priority
import org.zirbes.iceberg.model.Project

class PlaygroundSpec : ShouldSpec({

    val config = RepositoryConfig(
        bucket = "sandy-" + randomString(8).lowercase()
    )
    BucketInitializer(config.creds).bootstrap(config.bucket)
    val repository = ProjectTable(config)

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

    val gutters = Project(
        id = 3,
        name = "Finish building gutters",
        description = "Replace built in gutters with new copper",
        priority = Priority.HIGH,
        due = LocalDate.parse("2025-09-15")
    )

    should("be able to create projects") {
        //val repository = ProjectTable(config)

        repository.create(entry)
        println("Project created: $entry")
        repository.create(closet)
        repository.create(gutters)
        println("Project created: $closet and $gutters")
    }

    should("be able to retrieve a project") {
        //val repository = ProjectTable(config)
        val retrievedProject = repository.get(entry.id)

        println("Retrieved project: $retrievedProject")

        retrievedProject.shouldNotBeNull()
        retrievedProject.name shouldBe entry.name
    }

    should("be able to list projects") {
        //val repository = ProjectTable(config)
        val allProjects = repository.list()
        println("All projects: $allProjects")
        allProjects.forEach { project ->
            println(" * Project: $project")
        }

        allProjects.size shouldBe 3
    }

    should("be able to find projects") {
        //val repository = ProjectTable(config)
        val moveInDate = LocalDate.parse("2025-05-15")
        val upcomingProjects = repository.findDueBefore(moveInDate)
        val lowerPriorityProjects = repository.findPriorityLessThan(Priority.HIGH)
        val higherPriorityProjects = repository.findPriorityGreaterThan(Priority.LOW)

        println("Low priority projects: $lowerPriorityProjects")
        println("High priority projects: $higherPriorityProjects")
        println("Upcoming due projects: $upcomingProjects")

        assertSoftly {
            lowerPriorityProjects.size shouldBe 2
            higherPriorityProjects.size shouldBe 2
            upcomingProjects.size shouldBe 1
        }
    }

    // this is failing right now - need to learn more.
    should("be able to delete a project") {
        //val repository = ProjectTable(config)
        repository.snapshotInfo()
        repository.delete(entry)
        val remaining = repository.list()
        val remainingIs = remaining.map(Project::id)
        remaining.size shouldBe 2
        remainingIs shouldBe setOf(closet.id, gutters.id)
    }

    should("be able to maintain the database") {
        //val repository = ProjectTable(config)
        repository.maintain()
    }

})
