import com.googlecode.cqengine.attribute.Attribute
import com.googlecode.cqengine.index.Index
import com.googlecode.cqengine.index.support.AbstractAttributeIndex
import com.googlecode.cqengine.index.support.indextype.OnHeapTypeIndex
import com.googlecode.cqengine.persistence.support.ObjectSet
import com.googlecode.cqengine.persistence.support.ObjectStore
import com.googlecode.cqengine.query.Query
import com.googlecode.cqengine.query.option.QueryOptions
import com.googlecode.cqengine.query.simple.StringContains
import com.googlecode.cqengine.resultset.ResultSet
import kotlin.collections.ArrayList
import kotlin.math.min

class SuffixArray<V> private constructor(
    private val suffixArray: Array<SuffixAndValue<V>>,
    private val longestCommonPrefix: IntArray,
) {

    fun elementContainsSequence(str: String): Sequence<V> {
        val estimatedStreakStart = this.findLeftBound(str) + 1
        return if (suffixArray[estimatedStreakStart].startsWith(str)) {
            //found streak of at least 1
            Sequence {
                object : Iterator<V> {
                    var i = estimatedStreakStart

                    //if lcp[i] is greater than str.length ->
                    //suffixArray[i].suffix starts with at least str.length symbols of
                    //suffixArray[i - 1].suffix. We've checked suffixArray[i - 1] starts with str.length of the previous step ->
                    //can return the next one as well
                    override fun hasNext(): Boolean =
                        i < suffixArray.size && (longestCommonPrefix[i] >= str.length || i == estimatedStreakStart)

                    override fun next(): V = suffixArray[i++].value
                }
            }
        } else {
            emptySequence()
        }
    }

    fun elementEndsWithSequence(str: String): Sequence<V> {
        val leftBound = this.findLeftBound(str)
        return if (suffixArray[leftBound + 1].contentEquals(str)) {
            //found streak of at least 1
            Sequence {
                object : Iterator<V> {
                    var i = leftBound

                    override fun hasNext(): Boolean = i < suffixArray.size && (longestCommonPrefix[i] == str.length || i == leftBound)

                    override fun next(): V = suffixArray[i++].value
                }
            }
        } else {
            emptySequence()
        }
    }

    /**
     * @return index of the largest element smaller than [str]. Might return -1 if such element wasn't found
     */
    private fun findLeftBound(str: String): Int {
        @Suppress("UNCHECKED_CAST")
        val suffix = SuffixAndValue<V?>(str, 0, null) as SuffixAndValue<V>

        var low = 0
        var high = suffixArray.size - 1

        while (low < high) {
            val mid = (low + high).ushr(1) // safe from overflows
            val midVal = suffixArray[mid]
            val cmp = midVal.compareTo(suffix)

            if (cmp < 0)
                low = mid + 1
            else
                high = mid - 1
        }
        return if (suffixArray[low].startsWith(suffix)) low - 1 else low
    }

    companion object {
        fun <V> generateSuffixArray(
            elements: List<V>,
            mapFun: (V) -> Iterable<String>,
        ): SuffixArray<V> {
            val size = elements.sumOf { el -> mapFun(el).sumOf { it.length } }

            @Suppress("UNCHECKED_CAST")
            val intermediate = arrayOfNulls<SuffixAndValue<V>>(size) as Array<SuffixAndValue<V>>
            var outerI = 0
            elements.forEach { v ->
                val strings = mapFun(v)
                strings.forEach {
                    for (index in (it.indices)) {
                        intermediate[outerI++] = SuffixAndValue(it, index, v)
                    }
                }
            }
            intermediate.sort()

            // naive implementation of lcp (LongestCommonPrefix) - relatively fast compared to suffix array sorting
            val lcp = IntArray(intermediate.size)
            for (i in (1 until intermediate.size)) {
                lcp[i] = intermediate[i - 1].commonPrefixWithLength(intermediate[i])
            }
            return SuffixArray(intermediate, lcp)
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


class SuffixArraySnapshotIndex<O>(attribute: Attribute<O, String>) : OnHeapTypeIndex, AbstractAttributeIndex<String, O>(
    attribute,
    setOf(StringContains::class.java)
) {
    @Volatile
    private var suffixArray: SuffixArray<O> = SuffixArray.generateSuffixArray(emptyList()) { throw Exception() }

    override fun addAll(objectSet: ObjectSet<O>?, queryOptions: QueryOptions?): Boolean {
        throw UnsupportedOperationException("Index is read-only")
    }

    override fun removeAll(objectSet: ObjectSet<O>?, queryOptions: QueryOptions?): Boolean {
        throw UnsupportedOperationException("Index is read-only")
    }

    override fun clear(queryOptions: QueryOptions?) {}

    override fun init(objectStore: ObjectStore<O>?, queryOptions: QueryOptions?) {
        suffixArray = ObjectSet.fromObjectStore(objectStore, queryOptions).use { objectSet ->
            SuffixArray.generateSuffixArray(objectSet.toList()) { v -> attribute.getValues(v, queryOptions) }
        }
    }

    override fun destroy(queryOptions: QueryOptions?) {}

    @Suppress("UNCHECKED_CAST")
    override fun retrieve(query: Query<O>, queryOptions: QueryOptions): ResultSet<O> {
        val array = this.suffixArray
        return when (query.javaClass) {
            StringContains::class.java -> {
                query as StringContains<O, *>
                SuffixArrayResultSet(array, query.value.toString(), queryOptions, query)
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
        private val suffixArray: SuffixArray<O>,
        private val valueToSearch: String,
        private val queryOptions: QueryOptions,
        private val query: Query<O>
    ) : ResultSet<O>() {

        override fun iterator(): MutableIterator<O> {
            return suffixArray.elementContainsSequence(valueToSearch).toJavaIterator()
        }

        override fun contains(`object`: O): Boolean {
            return suffixArray.elementContainsSequence(valueToSearch).contains(`object`)
        }

        override fun matches(`object`: O): Boolean {
            return query.matches(`object`, queryOptions)
        }

        override fun size(): Int {
            return suffixArray.elementContainsSequence(valueToSearch).count()
        }

        override fun getRetrievalCost(): Int = 100

        override fun getMergeCost(): Int = 100

        override fun close() {
            // No op.
        }

        override fun getQuery(): Query<O> = query

        override fun getQueryOptions(): QueryOptions = queryOptions
    }

    companion object {

        fun <O> onAttribute(attribute: Attribute<O, String>): SuffixArraySnapshotIndex<O> {
            return SuffixArraySnapshotIndex(attribute)
        }
    }
}


fun <T> Sequence<T>.toJavaIterator(): MutableIterator<T> {
    val iter = this.iterator()
    return object : MutableIterator<T> {
        override fun hasNext(): Boolean = iter.hasNext()

        override fun next(): T = iter.next()

        override fun remove() {
            throw UnsupportedOperationException("remove")
        }
    }
}


private fun <V> mergeSimilar(intermediate: ArrayList<SuffixAndValue<V>>): ArrayList<SuffixAndValue<ArrayList<V>>> {
    val result = ArrayList<SuffixAndValue<ArrayList<V>>>()
    var cur = 1
    var prev = 0
    while (cur <= intermediate.size) {
        if (cur == intermediate.size || intermediate[cur] != intermediate[prev]) {
            //streak is [prev, cur - 1], let's merge
            val currArray = ArrayList<V>()
            for (i in (prev until cur)) {
                currArray.add(intermediate[i].value)
            }
            result.add(SuffixAndValue(intermediate[prev].str, intermediate[prev].from, currArray))
            prev = cur
        }
        cur++
    }
    return result
}

/**
 * Suffix and value merged into one class for space efficiency - suffix array has a lot of them, so sparse classes
 * (especially taking padding into account) can introduce a lot of overhead
 */
private class SuffixAndValue<V>(
    val str: String,
    val from: Int,
    val value: V
) : CharSequence, Comparable<SuffixAndValue<V>> {

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
        return "${str.substring(from)},$value"
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