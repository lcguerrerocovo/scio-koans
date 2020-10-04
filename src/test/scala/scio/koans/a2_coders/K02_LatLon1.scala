package scio.koans.a2_coders

import scio.koans.shared._

/**
 * Fix the non-deterministic coder exception.
 */
class K02_LatLon1 extends PipelineKoan {

  import K02_LatLon1._

  val events: Seq[(LatLon, String)] = Seq(
    (LatLon(34.5, -10.0), "a"),
    (LatLon(67.8, 12.3), "b"),
    (LatLon(67.8, 12.3), "c"),
    (LatLon(-45.0, 3.14), "d")
  )

  val expected: Seq[(LatLon, Set[String])] = Seq(
    (LatLon(34.5, -10.0), Set("a")),
    (LatLon(67.8, 12.3), Set("b", "c")),
    (LatLon(-45.0, 3.14), Set("d"))
  )

  "Snippet" should "work" in {
    runWithContext { sc =>
      val output = sc
        .parallelize(events)
        // `*ByKey` transforms compare keys using encoded bytes and must be deterministic
        .groupByKey
        .mapValues(_.toSet)

      output should containInAnyOrder(expected)
    }
  }
}

object K02_LatLon1 {
  // Latitude and longitude are degrees bounded by [-90, 90] and [-180, 180] respectively.
  // 1 degree = 60 minutes
  // 1 minute = 60 seconds
  // https://en.wikipedia.org/wiki/Decimal_degrees#Precision
  // Hint: `Double` encoding is not deterministic but `Int` is

  object LatLon {
    def toAngle(num : Double): Coordinate = {
      val (degree, minutesDecimal) = splitDouble(num)
      val (minutes, secondsDecimal) = splitDouble(minutesDecimal.getOrElse(0))
      Coordinate(
        degree.getOrElse(0),
        minutes.getOrElse(0),
        (secondsDecimal.getOrElse(0d)).toInt
      )
    }
    def splitDouble(num: Double): (Option[Int], Option[Double]) = num.toString.split('.') match {
      case Array(integer, decimal) =>
        (Some(integer.toInt), Some(("." + decimal).toDouble * 60))
      case Array(integer) => (Some(integer.toInt), None)
      case _ => (None, None)
    }

    def apply(lat: Double,lon: Double) = new LatLon(toAngle(lat),toAngle(lon))
  }

  case class Coordinate(degrees: Int, minutes: Int, seconds: Int)

  case class LatLon(lat: Coordinate,lon: Coordinate)
}
