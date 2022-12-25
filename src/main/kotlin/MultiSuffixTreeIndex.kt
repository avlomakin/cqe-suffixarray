import com.googlecode.cqengine.attribute.Attribute
import com.googlecode.cqengine.index.Index
import com.googlecode.cqengine.index.suffix.SuffixTreeIndex
import com.googlecode.cqengine.index.support.AbstractAttributeIndex
import com.googlecode.cqengine.index.support.CloseableIterator
import com.googlecode.cqengine.index.support.indextype.OnHeapTypeIndex
import com.googlecode.cqengine.persistence.support.ObjectSet
import com.googlecode.cqengine.persistence.support.ObjectStore
import com.googlecode.cqengine.query.Query
import com.googlecode.cqengine.query.option.QueryOptions
import com.googlecode.cqengine.query.simple.Equal
import com.googlecode.cqengine.query.simple.In
import com.googlecode.cqengine.query.simple.StringContains
import com.googlecode.cqengine.query.simple.StringEndsWith
import com.googlecode.cqengine.resultset.ResultSet
import com.googlecode.cqengine.resultset.connective.ResultSetUnionAll
import java.util.concurrent.ExecutorService

class MultiSuffixTreeIndex<A : CharSequence, O>(
    attribute: Attribute<O, A>,
    private val parallelDegree: Int,
    private val executor: ExecutorService,
) : OnHeapTypeIndex,
    AbstractAttributeIndex<A, O>(
        attribute,
        setOf(Equal::class.java, StringEndsWith::class.java, StringContains::class.java)
    ) {

    private val indices = (0 until parallelDegree).map { SuffixTreeIndex.onAttribute(attribute) }

    override fun init(objectStore: ObjectStore<O>?, queryOptions: QueryOptions?) {
        (0 until parallelDegree).map { index ->
            executor.submit {
                initBackingTree(index, objectStore, queryOptions)
            }
        }.forEach { it.get() }
    }


    override fun retrieve(query: Query<O>, queryOptions: QueryOptions): ResultSet<O> {
        return when (query::class.java) {
            Equal::class.java, In::class.java, StringContains::class.java -> ResultSetUnionAll(
                indices.map {
                    it.retrieve(query, queryOptions)
                }, query, queryOptions
            )
            else -> throw IllegalArgumentException("Unsupported query: $query")
        }
    }

    private fun initBackingTree(index: Int, objectStore: ObjectStore<O>?, queryOptions: QueryOptions?) {
        val tree = indices[index]
        val objectSet = FilteringObjectStore(ObjectSet.fromObjectStore(objectStore, queryOptions)) { i, _ ->
            i % parallelDegree == index
        }
        tree.addAll(objectSet, queryOptions)
    }

    override fun destroy(queryOptions: QueryOptions?) {
        //no-op
    }

    override fun isMutable(): Boolean = false

    override fun isQuantized(): Boolean = false

    override fun getEffectiveIndex(): Index<O> = this

    override fun addAll(objectSet: ObjectSet<O>?, queryOptions: QueryOptions?): Boolean {
        throw UnsupportedOperationException("index is immutable")
    }

    override fun removeAll(objectSet: ObjectSet<O>?, queryOptions: QueryOptions?): Boolean {
        throw UnsupportedOperationException("index is immutable")
    }

    override fun clear(queryOptions: QueryOptions?) {
        throw UnsupportedOperationException("index is immutable")
    }

    class FilteringObjectStore<O>(private val innerObjectSet: ObjectSet<O>, val predicate: (Int, O) -> Boolean) :
        ObjectSet<O>() {
        override fun iterator(): CloseableIterator<O> {
            val iterator = innerObjectSet.iterator()
            return object : CloseableIterator<O> {
                var nextState: Int = -1 // -1 for unknown, 0 for done, 1 for continue
                var nextItem: O? = null
                var nextIndex: Int = -1

                private fun calcNext() {
                    while (iterator.hasNext()) {
                        val item = iterator.next()
                        nextIndex++
                        if (predicate(nextIndex, item)) {
                            nextItem = item
                            nextState = 1
                            return
                        }
                    }
                    nextState = 0
                }

                override fun next(): O {
                    if (nextState == -1)
                        calcNext()
                    if (nextState == 0)
                        throw NoSuchElementException()
                    val result = nextItem
                    nextItem = null
                    nextState = -1
                    @Suppress("UNCHECKED_CAST")
                    return result as O
                }

                override fun hasNext(): Boolean {
                    if (nextState == -1)
                        calcNext()
                    return nextState == 1
                }

                override fun remove() {
                    throw UnsupportedOperationException()
                }

                override fun close() {
                    iterator.close()
                }
            }
        }

        override fun close() {
            innerObjectSet.close()
        }

        override fun isEmpty(): Boolean {
            return iterator().use { !it.hasNext() }
        }

    }
    companion object {

        fun <A : CharSequence, O> onAttribute(attribute: Attribute<O, A>, parallelDegree: Int, executor: ExecutorService): MultiSuffixTreeIndex<A, O> {
            return MultiSuffixTreeIndex(attribute, parallelDegree, executor)
        }
    }
}
