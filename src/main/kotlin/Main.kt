import com.abahgat.suffixtree.GeneralizedSuffixTree
import com.googlecode.cqengine.attribute.Attribute
import com.googlecode.cqengine.index.AttributeIndex
import com.googlecode.cqengine.index.Index
import com.googlecode.cqengine.index.suffix.SuffixTreeIndex
import com.googlecode.cqengine.index.support.AbstractAttributeIndex
import com.googlecode.cqengine.index.support.indextype.OnHeapTypeIndex
import com.googlecode.cqengine.persistence.support.ObjectSet
import com.googlecode.cqengine.persistence.support.ObjectStore
import com.googlecode.cqengine.query.Query
import com.googlecode.cqengine.query.option.QueryOptions
import com.googlecode.cqengine.resultset.ResultSet
import kotlin.random.Random

fun main(args: Array<String>) {

    val s = GeneralizedSuffixTree()

    val r = Random(0)
    val (_, ms) = measureTimeMs {
        var mark = System.currentTimeMillis()
        repeat(100000) {
            val next = r.nextString(20)
            if(it % 1000 == 0) {
                println(System.currentTimeMillis() - mark)
                mark = System.currentTimeMillis()
//                println(next)
            }
            s.put(next, it)
        }
    }


    println(ms)

    println(s.search("вща"))
    println(s.search("ффф"))
    println(s.search("11"))

}

private fun Random.nextString(i: Int) : String {
    return String(this.nextBytes(i).map { (it + 60).toByte() }.toByteArray())
}

fun <T> measureTimeMs(block: () -> T) : Pair<T, Long> {
    val start = System.currentTimeMillis()
    val result = block()
    return result to System.currentTimeMillis() - start
}

//
//class ReadonlyGstIndex<A : CharSequence, O>(attribute: Attribute<O, A>) : OnHeapTypeIndex,
//    AbstractAttributeIndex<A, O>(attribute, setOf(Contains)) {
//    override fun addAll(objectSet: ObjectSet<O>?, queryOptions: QueryOptions?): Boolean {
//        TODO("Not yet implemented")
//    }
//
//    override fun removeAll(objectSet: ObjectSet<O>?, queryOptions: QueryOptions?): Boolean {
//        TODO("Not yet implemented")
//    }
//
//    override fun clear(queryOptions: QueryOptions?) {
//        TODO("Not yet implemented")
//    }
//
//    override fun init(objectStore: ObjectStore<O>?, queryOptions: QueryOptions?) {
//        TODO("Not yet implemented")
//    }
//
//    override fun destroy(queryOptions: QueryOptions?) {
//        TODO("Not yet implemented")
//    }
//
//    override fun isMutable(): Boolean = false
//
//    override fun isQuantized(): Boolean {
//        TODO("Not yet implemented")
//    }
//
//    override fun retrieve(query: Query<O>?, queryOptions: QueryOptions?): ResultSet<O> {
//        TODO("Not yet implemented")
//    }
//
//    override fun getEffectiveIndex(): Index<O> {
//        TODO("Not yet implemented")
//    }
//
//}