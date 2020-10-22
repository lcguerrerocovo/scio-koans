package scio.koans.b1_reduce

import com.spotify.scio.values.SCollection
import scio.koans.shared._

import scala.collection.mutable

/**
 * Optimize `{fold,combine,aggregate}ByKey` with mutable collections.
 */
class K13_MutableFoldCombineAggregate extends TransformKoan {

  type InT = SCollection[(String, Int)]
  type OutT = SCollection[(String, Int)]

  val input: Seq[(String, Int)] = Seq(
    ("a", 1),
    ("a", 1),
    ("a", 1),
    ("a", 2),
    ("a", 2),
    ("a", 3),
    ("b", 4),
    ("b", 5),
    ("c", 6)
  )
  val expected: Seq[(String, Int)] = Seq(("a", 3), ("b", 2), ("c", 1))

  prepare(_.parallelize(input))
  verify(_ should containInAnyOrder(expected))

  baseline { input =>
    input.mapValues(Set(_)).sumByKey.mapValues(_.size)
  }

  test("foldByKey") { input =>
    // Hint: mutate LHS instead of creating a new collection
    input.mapValues(mutable.Set(_))
      .foldByKey(mutable.Set.empty) {
        (l,r) => l += r.head
      }
      .mapValues(_.size)
  }

  test("combineByKey") { input =>
    input.combineByKey(mutable.Set(_)) {
      (acc,v) => acc += v
    } {
      (acc, vs) => vs.foreach(v => acc += v); acc
    }.mapValues(_.size)
  }

  test("aggregateByKey") { input =>
    def seqOp(accum: mutable.Set[Int], v: Int): mutable.Set[Int] = ???
    def combOp(x: mutable.Set[Int], y: mutable.Set[Int]): mutable.Set[Int] = ???
    input.aggregateByKey(mutable.Set.empty[Int])(
      (acc,v) => acc += v
    , (acc, vs) => {vs.foreach(v => acc += v); acc}
    )
    .mapValues(_.size)
  }
}
