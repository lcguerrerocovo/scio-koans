package scio.koans.a2_coders

import com.spotify.scio.coders.Coder
import scio.koans.shared._
import shapeless.syntax.std.tuple._

/**
 * Fix the non-deterministic coder exception.
 */
class K03_LatLon2 extends PipelineKoan {
  ImNotDone

  import K03_LatLon2._

  val events: Seq[(LatLon, String)] = Seq(
    (LatLon(34.5, -10.0), "a"),
    (LatLon(67.8, 12.3), "b"),
    (LatLon(67.8, 12.3), "c"),
    (LatLon(-45.0, 3.14), "d")
  )

  val expected: Seq[(K03_LatLon2.LatLon, Set[String])] = Seq(
    (LatLon(34.5, -10.0), Set("a")),
    (LatLon(67.8, 12.3), Set("b", "c")),
    (LatLon(-45.0, 3.14), Set("d"))
  )

  "Snippet" should "work" in {
    runWithContext { sc =>
      val output = sc
        .parallelize(events)
        .groupByKey
        .mapValues(_.toSet)

      output should containInAnyOrder(expected)
    }
  }
}

object K03_LatLon2 {
  case class LatLon(lat: Double, lon: Double)

  import K02_LatLon1._

  object LatLon {

    val c: Double = 1/60
    def toDouble(d: Int, m: Int, s: Int): Double = {
      d + (m * c) + (s * math.pow(c,2))
    }
    // Instead of changing the definition of `LatLon`, define a custom coder
    // Companion object of `LatLon` is searched for implicit `F[LatLon]`, i.e. `Coder[LatLon]`
    // https://docs.scala-lang.org/tutorials/FAQ/finding-implicits.html
    // Hint: derive a `LatLon` coder from a type with deterministic encoding
    implicit val latLonCoder: Coder[LatLon]
    = Coder.xmap(Coder[((Int,Int,Int),(Int,Int,Int))])(
      (i: ((Int,Int,Int),(Int,Int,Int))) =>
        LatLon((toDouble _).tupled(i._1),(toDouble _).tupled(i._2))
      ,
      (j: LatLon) => {
        (K02_LatLon1.LatLon.toAngle(j.lat),K02_LatLon1.LatLon.toAngle(j.lon)) match {
          case (Coordinate(d,m,s),Coordinate(d2,m2,s2)) =>
            ((d,m,s),(d2,m2,s2))
        }
      }
    )
  }
}
