package org.zirbes.iceberg

import io.kotest.property.Arb
import io.kotest.property.RandomSource
import io.kotest.property.arbitrary.Codepoint
import io.kotest.property.arbitrary.alphanumeric
import io.kotest.property.arbitrary.string

fun randomString(size: Int = 10) = Arb.string(size, Codepoint.alphanumeric()).sample(RandomSource.default()).value
