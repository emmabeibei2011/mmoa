package com.mediamath.oa

import org.apache.spark.sql.Row
import org.scalatest.FlatSpec

class SolutionTest extends FlatSpec {

  "eventsToKv() and impsToKv()" should "parse correctly to tuple." in {
    assert(Solution.eventsToKv("1234567,event1,ad1,user1,purchase")
      == (("ad1", "user1"), ("1234567", "purchase")))

    assert(Solution.eventsToKv("1234567,event1,ad1,user1,purchase")
      == (("ad1", "user1"), ("1234567", "purchase")))
  }


  "eventsToKv() and impsToKv()" should "throw Exception if schema is not correct" in {
    assertThrows[IllegalArgumentException] { Solution.eventsToKv("1234567,event1,ad1,user1,purchase,a")}
    assertThrows[IllegalArgumentException] { Solution.eventsToKv("aaa") }
    assertThrows[IllegalArgumentException] { Solution.impsToKv("1234567,ad1,cr1,user1,a")}
    assertThrows[IllegalArgumentException] { Solution.impsToKv("bbb") }
  }


  "filterAttributedEvent()" should "correctly filter out attributed events" in {
    val row1 = List((("ad1", "user1"), List(
        ("9998", "click"),
        ("10001", "impression"),
        ("10015", "click"),
        ("10025", "click"),
        ("10035", "impression"),
        ("10045", "click"),
        ("10075", "click"),
        ("10085", "click")
      )))
    val row1expected = List(
      Row("ad1", "user1", "10015", "click"),
      Row("ad1", "user1", "10075", "click")
    )
    assert(Solution.filterAttributedEvent(row1.toIterator).toList == row1expected)

    val row2 = List((("ad2", "user2"), List(
        ("9998", "click"),
        ("100001", "impression"),
        ("100015", "purchase"),
        ("100018", "impression"),
        ("100025", "click"),
        ("100035", "purchase"),
        ("100045", "impression"),
        ("100065", "visit"),
        ("100095", "visit"),
        ("100110", "impression"),
        ("100120", "visit"),
        ("100140", "visit")
      )))
    val row2expected = List(
      Row("ad2", "user2", "100015", "purchase"),
      Row("ad2", "user2", "100025", "click"),
      Row("ad2", "user2", "100065", "visit"),
      Row("ad2", "user2", "100140", "visit")
    )
    assert(Solution.filterAttributedEvent(row2.toIterator).toList == row2expected)

  }
}
