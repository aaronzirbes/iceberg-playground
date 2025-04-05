package org.zirbes.iceberg.dao

import java.time.Instant
import java.time.LocalDate
import java.util.UUID
import java.util.concurrent.TimeUnit
import org.apache.iceberg.AppendFiles
import org.apache.iceberg.DataFile
import org.apache.iceberg.DataFiles
import org.apache.iceberg.DeleteFile
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Schema
import org.apache.iceberg.Table
import org.apache.iceberg.Transaction
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.IcebergGenerics
import org.apache.iceberg.data.Record
import org.apache.iceberg.data.parquet.GenericParquetWriter
import org.apache.iceberg.deletes.EqualityDeleteWriter
import org.apache.iceberg.expressions.Expression
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.io.DataWriter
import org.apache.iceberg.io.OutputFile
import org.apache.iceberg.parquet.Parquet
import org.apache.iceberg.types.Types
import org.zirbes.iceberg.config.HadoopCatalogBuilder
import org.zirbes.iceberg.config.RepositoryConfig
import org.zirbes.iceberg.model.Priority
import org.zirbes.iceberg.model.Project

class ProjectTable(
    private val catalog: HadoopCatalog,
    namespace: String = "default",
) {

    constructor(config: RepositoryConfig) :
        this(HadoopCatalogBuilder(config).build(), config.namespace)

    private val tableIdentifier = TableIdentifier.of(namespace, TABLE_NAME)
    private val table: Table = createIfNotExists()
    private val spec = table.spec()

    fun create(project: Project) = create(listOf(project))

    fun create(projects: Collection<Project>) = table.withTransaction { tx ->
        val append = tx.newAppend()
        append.appendFile(
            projects.map(this::toRecord).writeToFile()
        )
        append.commit()
    }

    fun list(): List<Project> {
        val scan = IcebergGenerics.read(table).build()
        val records = scan.iterator()
        val found = mutableListOf<Project>()
        records.forEach { record ->
            val project = record.toProject()
            println("Found project: $project")
            found.add(project)
        }
        return found
    }

    fun get(id: Int): Project? = getRecord(id)?.toProject()

    fun findPriorityLessThan(priority: Priority): List<Project> =
        findWhere(Expressions.lessThan("priority", priority.intValue))

    fun findPriorityGreaterThan(priority: Priority): List<Project> =
        findWhere(Expressions.greaterThan("priority", priority.intValue))

    fun findDueBefore(due: LocalDate): List<Project> {
        return list()
            .filter { it.due != null && it.due.isBefore(due) }
    }

    fun maintain() {
        val thirtyDays = TimeUnit.DAYS.toMillis(30)
        val tenMegaBytes = 10 * 1024 * 1024
        table.withTransaction { tx ->
            tx.rewriteManifests()
                .rewriteIf { file -> file.length() < tenMegaBytes }
                .commit()
            tx.expireSnapshots()
                .expireOlderThan(thirtyDays)
                .commit()
        }
    }

    fun delete(projects: Collection<Project>) = table.withTransaction { tx ->
        val deleteOp = tx.newRowDelta()
        deleteOp.addDeletes(projects.deleteToFile())
        deleteOp.commit()
    }

    fun delete(project: Project) {
        delete(listOf(project))
    }

    fun snapshotInfo() {
        val snapshot = table.currentSnapshot()
        if (snapshot != null) {
            println("Current snapshot ID: ${snapshot.snapshotId()}")
            println("Snapshot timestamp: ${Instant.ofEpochMilli(snapshot.timestampMillis())}")
            println("Summary:")
            snapshot.summary().forEach { t, u ->
                println("  $t: $u")
            }
        }
    }


    private fun List<Record>.writeToFile(): DataFile {
        val outputFile = newOutput()
        // Create a data writer for the file
        val writer = dataWriter(outputFile)
        writer.write(this)
        writer.close()

        // Add the file to the append operation
        return DataFiles.builder(spec)
            .withPath(outputFile.location())
            .withFileSizeInBytes(writer.length())
            .withRecordCount(this.size.toLong())
            .build()
    }

    private fun Collection<Project>.deleteToFile(): DeleteFile {
        val outputFile = newOutput()
        val records = this.map(::toRecord)
        // Create a data writer for the file
        val deleter = dataDeleter(outputFile)
        deleter.write(records)
        deleter.close()
        return deleter.toDeleteFile()
    }

    private fun dataWriter(outputFile: OutputFile): DataWriter<Record> =
        Parquet.writeData(outputFile)
            .schema(TABLE_SCHEMA)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .overwrite()
            .withSpec(spec)
            .build()

    private fun dataDeleter(outputFile: OutputFile): EqualityDeleteWriter<Record> =
        Parquet.writeDeletes(outputFile)
            .forTable(table)
            .equalityFieldIds(listOf(1))
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .overwrite()
            .buildEqualityWriter()

    private fun getRecord(id: Int): Record? {
        val results = IcebergGenerics.read(table)
            .where(Expressions.equal("id", id))
            .build()
            .iterator()
        return if (results.hasNext()) results.next() else null
    }

    private fun findWhere(clause: Expression): List<Project> {
        val results = mutableListOf<Project>()
        IcebergGenerics.read(table)
            .where(clause)
            .build()
            .iterator()
            .forEach { record -> results.add(record.toProject()) }
        return results.toList()
    }

    private fun newOutput(): OutputFile = table.io().newOutputFile(
        "${table.location()}/data/${UUID.randomUUID()}.parquet"
    )

    private fun createIfNotExists(): Table =
        if (catalog.tableExists(tableIdentifier)) {
            println("ℹ️ Table already exists.")
            catalog.loadTable(tableIdentifier)
        } else {
            println("⚠️ Creating table.")
            catalog.createTable(tableIdentifier, TABLE_SCHEMA, UNPARTITIONED, TABLE_PROPERTIES)
        }

    @Suppress("MagicNumber")
    private fun toRecord(project: Project): Record {
        val record = GenericRecord.create(TABLE_SCHEMA)
        record.setField("id", project.id)
        record.setField("name", project.name)
        record.setField("description", project.description)
        record.setField("priority", project.priority.intValue)
        record.setField("due", project.due)
        return record
    }

    private fun Record.toProject(): Project =
        Project(
            id = this.getField("id") as Int,
            name = this.getField("name") as String,
            description = this.getField("description") as String,
            priority = Priority.fromValue(this.getField("priority") as Int),
            due = this.getField("due") as LocalDate,
        )

    companion object {

        private val TABLE_NAME = "projects"
        private val UNPARTITIONED = PartitionSpec.unpartitioned()

        const val COPY_ON_WRITE = "copy-on-write"
        //const val MERGE_ON_READ = "merge-on-read"

        val TABLE_SCHEMA = Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "description", Types.StringType.get()),
            Types.NestedField.required(4, "priority", Types.IntegerType.get()),
            Types.NestedField.optional(5, "due", Types.DateType.get())
        )

        val DELETE_SCHEMA = Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
        )

        val TABLE_PROPERTIES = mapOf(
            "write.delete.mode" to COPY_ON_WRITE,
            "write.format.default" to "parquet",
            "write.parquet.compression-codec" to "snappy",
            "read.parquet.enable_vectorized_reader" to "true"
        )
    }
}

fun <T> Table.withTransaction(function: (Transaction) -> T) {
    val tx: Transaction = this.newTransaction()
    function(tx)
    tx.commitTransaction()
}

fun Table.append(function: () -> DataFile) =
    this.withTransaction { tx ->
        val appendOp: AppendFiles = tx.newAppend()
        appendOp.appendFile(function())
        appendOp.commit()
    }
