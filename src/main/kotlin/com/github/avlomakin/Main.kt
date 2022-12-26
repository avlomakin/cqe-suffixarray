package com.github.avlomakin

import com.googlecode.cqengine.ConcurrentIndexedCollection
import com.googlecode.cqengine.IndexedCollection
import com.googlecode.cqengine.attribute.support.FunctionalMultiValueAttribute
import com.googlecode.cqengine.index.AttributeIndex
import com.googlecode.cqengine.index.suffix.SuffixTreeIndex
import kotlin.math.abs
import kotlin.random.Random


val attribute = FunctionalMultiValueAttribute(Element::class.java, String::class.java, "aliases") { it.aliases }

fun main(args: Array<String>) {

    val times = 20
    val index = { ImmutableSuffixArrayIndex.onAttribute(attribute) }
    println(
        """
        run creation benchmark $times times, index: ${index::class.simpleName}
    """.trimIndent()
    )

    val results = LongArray(times)
    repeat(times) {
        val (r, nano) = measureTimeNano { CollectionGenerationBenchmark(index()).generateCollection() }
        print("${r.size} ")
        results[it] = nano
    }
    println(
        """
        avg = ${results.slice(2 until times).average() / 100000} ms 
    """.trimIndent()
    )
}


open class CollectionGenerationBenchmark(
    val index: AttributeIndex<String, Element>,
    val elementToPersistCount: Int = 100_000
) {

    fun generateCollection(): IndexedCollection<Element> {
        val collection = ConcurrentIndexedCollection<Element>()
        val r = Random(0)
        collection.addAll((0 until elementToPersistCount).map {
            Element(
                it.toLong(),
                setOf(r.nextString(20), r.nextString(30))
            )
        })

        collection.addIndex(index)
        return collection
    }
}

fun <T> measureAndLog(message: String, block: () -> T): T {
    println("$message started...")
    val (ret, ms) = measureTimeMs(block)
    println("$message finished in $ms ms")
    return ret
}

fun Random.nextString(i: Int): String {
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