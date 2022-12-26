package com.github.avlomakin

import com.googlecode.cqengine.attribute.Attribute
import com.googlecode.cqengine.index.Index
import com.googlecode.cqengine.index.support.AbstractAttributeIndex
import com.googlecode.cqengine.index.support.indextype.OnHeapTypeIndex
import com.googlecode.cqengine.persistence.support.ObjectSet
import com.googlecode.cqengine.persistence.support.ObjectStore
import com.googlecode.cqengine.query.Query
import com.googlecode.cqengine.query.option.QueryOptions
import com.googlecode.cqengine.query.simple.StringContains
import com.googlecode.cqengine.query.simple.StringEndsWith
import com.googlecode.cqengine.resultset.ResultSet
import kotlin.math.min
import com.googlecode.cqengine.index.suffix.SuffixTreeIndex

/**
 * Suffix Array + LCP to
 */
class ImmutableSuffixArray<V> private constructor(
    private val suffixes: Array<SuffixAndValue<V>>,
    private val lcp: IntArray,
) {

    /**
     * might return the same element multiple times
     */
    fun elementContainsSequence(str: String): Sequence<V> {
        val estimatedStreakStart = this.findLeftBound(str) + 1
        return if (estimatedStreakStart < suffixes.size && suffixes[estimatedStreakStart].startsWith(str)) {
            //found streak of at least 1
            Sequence {
                object : Iterator<Sequence<V>> {
                    var i = estimatedStreakStart

                    //if lcp[i] is greater than str.length ->
                    //suffixes[i] starts with at least str.length symbols of
                    //suffixes[i - 1]. We've checked suffixes[i - 1] starts with str.length of the previous step ->
                    //can return the next one as well
                    override fun hasNext(): Boolean =
                        i < suffixes.size && (lcp[i] >= str.length || i == estimatedStreakStart)

                    override fun next(): Sequence<V> = suffixes[i++].value()
                }
            }.flatten()
        } else {
            emptySequence()
        }
    }

    fun elementEndsWithSequence(str: String): Sequence<V> {
        val leftBound = this.findLeftBound(str)
        return if (leftBound < suffixes.size && suffixes[leftBound + 1].contentEquals(str)) {
            //found streak of at least 1
            Sequence {
                object : Iterator<Sequence<V>> {
                    var i = leftBound

                    override fun hasNext(): Boolean =
                        i < suffixes.size && (lcp[i] == str.length || i == leftBound)

                    override fun next(): Sequence<V> = suffixes[i++].value()
                }
            }.flatten()
        } else {
            emptySequence()
        }
    }

    /**
     * @return index of the largest element smaller than [str]. Returns -1 or [suffixes].size - 1 if such element isn't found
     */
    private fun findLeftBound(str: String): Int {
        @Suppress("UNCHECKED_CAST")
        val suffix = SuffixAndOneValue<V?>(str, 0, null) as SuffixAndValue<V>

        var low = 0
        var high = suffixes.size - 1

        while (low < high) {
            val mid = (low + high).ushr(1) // safe from overflows
            val midVal = suffixes[mid]
            val cmp = midVal.compareTo(suffix)

            if (cmp < 0)
                low = mid + 1
            else
                high = mid - 1
        }
        return if (suffixes[low].startsWith(suffix)) low - 1 else low
    }

    companion object {

        @Suppress("UNCHECKED_CAST")
        private fun <V> ImmutableSuffixArray<V>.mergeSameSuffixes(): ImmutableSuffixArray<V> {
            if (this.suffixes.size <= 1) {
                return this
            }

            var estimatedSize = 1
            for (i in (1 until this.suffixes.size)) {
                if (!isSuffixTheSameAsPrevious(this, i)) {
                    estimatedSize++
                }
            }

            val mergedSuffixes = arrayOfNulls<SuffixAndValue<V>>(estimatedSize) as Array<SuffixAndValue<V>>
            val mergedLcp = IntArray(estimatedSize)

            var cur = 1
            var prev = 0
            var i = 0
            while (cur <= this.suffixes.size) {
                if (cur == this.suffixes.size || !isSuffixTheSameAsPrevious(this, cur)) {
                    //streak is [prev, cur - 1], let's merge
                    when (cur - prev) {
                        1 -> {
                            mergedSuffixes[i] = this.suffixes[prev] as SuffixAndOneValue<V>
                            mergedLcp[i] = this.lcp[prev]
                        }
                        2 -> {
                            val prevSuf = this.suffixes[prev] as SuffixAndOneValue<V>
                            val prevPlusOneSuf = this.suffixes[prev + 1] as SuffixAndOneValue<V>
                            mergedSuffixes[i] =
                                SuffixAndTwoValues(prevSuf.str, prevSuf.from, prevSuf.value, prevPlusOneSuf.value)
                            mergedLcp[i] = this.lcp[prev]
                        }
                        else -> {
                            val prevSuf = this.suffixes[prev] as SuffixAndOneValue<V>
                            mergedSuffixes[i] = SuffixAndValueList(
                                prevSuf.str,
                                prevSuf.from,
                                this.suffixes.slice(prev until cur).map { (it as SuffixAndOneValue<V>).value }
                            )
                            mergedLcp[i] = this.lcp[prev]
                        }
                    }
                    i++
                    prev = cur
                }
                cur++
            }
            return ImmutableSuffixArray(mergedSuffixes, mergedLcp)
        }

        private fun <V> isSuffixTheSameAsPrevious(suffixArray: ImmutableSuffixArray<V>, i: Int) =
            (suffixArray.suffixes[i].length == suffixArray.suffixes[i - 1].length
                    && suffixArray.lcp[i] == suffixArray.suffixes[i].length)

        fun <V> generateSuffixArray(
            elements: List<V>,
            mapFun: (V) -> Iterable<String>,
        ): ImmutableSuffixArray<V> {
            val size = elements.sumOf { el -> mapFun(el).sumOf { it.length } }

            @Suppress("UNCHECKED_CAST")
            val intermediate = arrayOfNulls<SuffixAndValue<V>>(size) as Array<SuffixAndValue<V>>
            var outerI = 0
            elements.forEach { v ->
                val strings = mapFun(v)
                strings.forEach {
                    for (index in (it.indices)) {
                        intermediate[outerI++] = SuffixAndOneValue(it, index, v)
                    }
                }
            }
            intermediate.sort()

            // naive implementation of lcp (LongestCommonPrefix) - relatively fast compared to suffix array sorting
            val lcp = IntArray(intermediate.size)
            for (i in (1 until intermediate.size)) {
                lcp[i] = intermediate[i - 1].commonPrefixWithLength(intermediate[i])
            }
            return ImmutableSuffixArray(intermediate, lcp)
        }

        private fun <V> SuffixAndValue<V>.commonPrefixWithLength(other: SuffixAndValue<V>): Int {
            val shortestLength = minOf(this.length, other.length)

            var i = 0
            while (i < shortestLength && this[i] == other[i]) {
                i++
            }
            return i
        }
    }
}

/**
 * Index based on [ImmutableSuffixArray] to be used with the immutable [com.googlecode.cqengine.IndexedCollection]
 * This class throws exceptions on any modification attempt. Takes far less space than [SuffixTreeIndex] &
 * faster to construct. Retrieval is done in O(m * log(n) + k), where n - number of suffixes, k - number of values returned,
 * m - length of substring searched.
 *
 * Supported queries:
 * 1. [StringContains]
 *
 * Usage:
 * ```
 *     val collection = ConcurrentIndexedCollection<SomeElement>()
 *     collection.addAll(elements)
 *
 *     val index = ImmutableSuffixArrayIndex.onAttribute(attribute)
 *     collection.addIndex(index)
 * ```
 */
class ImmutableSuffixArrayIndex<O>(attribute: Attribute<O, String>) : OnHeapTypeIndex, AbstractAttributeIndex<String, O>(
    attribute,
    setOf(StringContains::class.java)
) {
    @Volatile
    private var suffixArray: ImmutableSuffixArray<O> = ImmutableSuffixArray.generateSuffixArray(emptyList()) { throw Exception() }

    override fun addAll(objectSet: ObjectSet<O>?, queryOptions: QueryOptions?): Boolean {
        throw UnsupportedOperationException("Index is read-only. Call addIndex() after adding all elements")
    }

    override fun removeAll(objectSet: ObjectSet<O>?, queryOptions: QueryOptions?): Boolean {
        throw UnsupportedOperationException("Index is read-only. Call addIndex() after removing all elements")
    }

    override fun clear(queryOptions: QueryOptions?) {}

    override fun init(objectStore: ObjectStore<O>?, queryOptions: QueryOptions?) {
        suffixArray = ObjectSet.fromObjectStore(objectStore, queryOptions).use { objectSet ->
            ImmutableSuffixArray.generateSuffixArray(objectSet.toList()) { v -> attribute.getValues(v, queryOptions) }
        }
    }

    override fun destroy(queryOptions: QueryOptions?) {}

    @Suppress("UNCHECKED_CAST")
    override fun retrieve(query: Query<O>, queryOptions: QueryOptions): ResultSet<O> {
        val array = this.suffixArray
        return when (query.javaClass) {
            StringContains::class.java -> {
                query as StringContains<O, *>
                val str = query.value.toString()
                SuffixArrayResultSet(queryOptions, query) {
                    array.elementContainsSequence(str)
                }
            }
            StringEndsWith::class.java -> {
                query as StringEndsWith<O, *>
                val str = query.value.toString()
                SuffixArrayResultSet(queryOptions, query) {
                    array.elementEndsWithSequence(str)
                }
            }
            else -> {
                throw IllegalArgumentException("Unsupported query: $query")
            }
        }
    }

    override fun getEffectiveIndex(): Index<O> = this

    override fun isMutable(): Boolean = false

    override fun isQuantized(): Boolean = false

    class SuffixArrayResultSet<O>(
        private val queryOptions: QueryOptions,
        private val query: Query<O>,
        private val sequenceProducer: () -> Sequence<O>,
    ) : ResultSet<O>() {

        override fun iterator(): MutableIterator<O> {
            val iter = sequenceProducer().iterator()
            return object : MutableIterator<O> {
                override fun hasNext(): Boolean = iter.hasNext()

                override fun next(): O = iter.next()

                override fun remove() {
                    throw UnsupportedOperationException("remove")
                }
            }
        }

        override fun contains(`object`: O): Boolean {
            return sequenceProducer().contains(`object`)
        }

        override fun matches(`object`: O): Boolean {
            return query.matches(`object`, queryOptions)
        }

        override fun size(): Int {
            return sequenceProducer().count()
        }

        override fun getRetrievalCost(): Int = 100

        override fun getMergeCost(): Int = sequenceProducer().count()

        override fun close() {
            // No op.
        }

        override fun getQuery(): Query<O> = query

        override fun getQueryOptions(): QueryOptions = queryOptions
    }

    companion object {
        fun <O> onAttribute(attribute: Attribute<O, String>): ImmutableSuffixArrayIndex<O> {
            return ImmutableSuffixArrayIndex(attribute)
        }
    }
}
/**
 * Suffix and value merged into one class for space efficiency - suffix array has a lot of them, so sparse classes
 * (especially taking padding into account) can introduce a lot of overhead
 */
abstract class SuffixAndValue<V>(
    val str: String,
    val from: Int,
) : CharSequence, Comparable<SuffixAndValue<V>> {

    abstract fun value(): Sequence<V>

    override val length: Int
        get() = str.length - from

    override fun get(index: Int): Char {
        return str[index + from]
    }

    override fun subSequence(startIndex: Int, endIndex: Int): CharSequence {
        return str.subSequence(startIndex + from, endIndex + from)
    }

    /**
     * NB: we're only comparing suffixes, so objects with different [value] fields might be equal
     */
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as SuffixAndValue<*>
        if (length != other.length) return false
        for (index in indices.reversed()) {
            if (this[index] != other[index]) return false
        }

        return true
    }

    override fun hashCode(): Int {
        //might re-calc if hashCode is 0
        var h: Int = 0
        for (c in (from until str.length)) {
            h = 31 * h + str[c].code
        }
        return h
    }

    override fun toString(): String {
        return "${str.substring(from)},${value().joinToString()}"
    }

    override fun compareTo(other: SuffixAndValue<V>): Int {
        val minLength = min(length, other.length)
        for (i in (0 until minLength)) {
            val char1 = get(i)
            val char2 = other[i]
            if (char1.code != char2.code) {
                return char1.code - char2.code
            }
        }
        return length - other.length
    }
}

private class SuffixAndOneValue<V>(
    str: String,
    from: Int,
    val value: V
) : SuffixAndValue<V>(str, from) {
    override fun value(): Sequence<V> = sequenceOf(value)
}

private class SuffixAndTwoValues<V>(
    str: String,
    from: Int,
    val first: V,
    val second: V
) : SuffixAndValue<V>(str, from) {
    override fun value(): Sequence<V> = sequenceOf(first, second)
}

private class SuffixAndValueList<V>(
    str: String,
    from: Int,
    val array: List<V>,
) : SuffixAndValue<V>(str, from) {
    override fun value(): Sequence<V> = array.asSequence()
}