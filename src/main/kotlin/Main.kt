import com.googlecode.cqengine.ConcurrentIndexedCollection
import com.googlecode.cqengine.attribute.support.FunctionalMultiValueAttribute
import com.googlecode.cqengine.index.AttributeIndex
import com.googlecode.cqengine.index.radix.RadixTreeIndex
import com.googlecode.cqengine.index.suffix.SuffixTreeIndex
import com.googlecode.cqengine.query.QueryFactory
import kotlin.math.abs
import kotlin.random.Random


val attribute = FunctionalMultiValueAttribute(Element::class.java, String::class.java, "aliases") { it.aliases }

fun main(args: Array<String>) {
    val tree = SuffixArraySnapshotIndex.onAttribute(attribute)
    testPackFor(tree)
}

fun testPackFor(
    index: AttributeIndex<String, Element>,
    elementToPersistCount: Int = 100_000,
    persistedElementsUsedInTest: Int = 1_000,
    notFoundElementsCount: Int = 1_000,
    referenceIndex: AttributeIndex<String, Element>? = null
) {
    println("run for $elementToPersistCount, $index")
    val collectionUnderTesting = ConcurrentIndexedCollection<Element>()
    val reference = ConcurrentIndexedCollection<Element>()
    val r = Random(0)
    val elements = measureAndLog("initial preparation") {
        (0 until elementToPersistCount).map { Element(it.toLong(), setOf(r.nextString(20), r.nextString(30))) }
            .also {
                collectionUnderTesting.addAll(it)
                reference.addAll(it)
            }
    }

    measureAndLog("adding Index") {
        collectionUnderTesting.addIndex(index)
    }

    if (referenceIndex != null) {
        measureAndLog("adding reference Index") {
            reference.addIndex(referenceIndex)
        }
    }

    measureAndLog("warming up retrieval") {
        elements.asSequence().take(1000).forEach {
            val query = QueryFactory.contains(attribute, it.aliases.first().take(4))
            collectionUnderTesting.retrieve(query).map { it.id }
            reference.retrieve(query).map { it.id }
        }
    }

    val shuffled = elements.shuffled(r)
    var actualDurationNano = 0L
    var expectedDurationNano = 0L
    measureAndLog("testing existing") {
        shuffled.asSequence().take(persistedElementsUsedInTest)
            .flatMap { el ->
                el.aliases.map {
                    val length = r.nextInt(3, 10)
                    val start = r.nextInt(0, 6)
                    it.substring(start, start + length)
                }
            }
            .forEach { substring ->
                val query = QueryFactory.contains(attribute, substring)
                val (actualResult, actualQueryDurationNano) = measureTimeNano {
                    collectionUnderTesting.retrieve(query).map { it.id }
                }
                val (expectedResult, expectedQueryDurationNano) = measureTimeNano {
                    reference.retrieve(query).map { it.id }
                }
                check(actualResult.toSet() == expectedResult.toSet()) { "check failed for $substring" }
                actualDurationNano += actualQueryDurationNano
                expectedDurationNano += expectedQueryDurationNano
            }
    }

    println(
        """
        | reference: Total: ${expectedDurationNano / 1000000} ms, ${persistedElementsUsedInTest / (expectedDurationNano / 1000000)} qpms
        |       CUT: Total: ${actualDurationNano / 1000000} ms, ${persistedElementsUsedInTest / (actualDurationNano / 1000000)} qpms
        |
    """.trimMargin()
    )


    measureAndLog("checking non-existing") {
        shuffled.asSequence().drop(persistedElementsUsedInTest).take(notFoundElementsCount).forEach { el ->
            val retrieve =
                collectionUnderTesting.retrieve(
                    QueryFactory.contains(
                        attribute,
                        el.aliases.first().substring(5, 9) + "$"
                    )
                )
            check(retrieve.isEmpty) { "check failed for $el" }
        }
    }
    Thread.sleep(1000000000)
    collectionUnderTesting.toString()
}


fun <T> measureAndLog(message: String, block: () -> T): T {
    println("$message started...")
    val (ret, ms) = measureTimeMs(block)
    println("$message finished in $ms ms")
    return ret
}

private fun Random.nextString(i: Int): String {
    return String(this.nextBytes(i).map { (65 + abs(it % 15)).toByte() }.toByteArray())
}

fun <T> measureTimeMs(block: () -> T): Pair<T, Long> {
    val start = System.currentTimeMillis()
    val result = block()
    return result to System.currentTimeMillis() - start
}

fun <T> measureTimeNano(block: () -> T): Pair<T, Long> {
    val start = System.nanoTime()
    val result = block()
    return result to System.nanoTime() - start
}

data class Element(
    val id: Long,
    val aliases: Set<String>
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Element

        if (id != other.id) return false

        return true
    }

    override fun hashCode(): Int {
        return id.hashCode()
    }

    override fun toString(): String {
        return "(id=$id)"
    }
}