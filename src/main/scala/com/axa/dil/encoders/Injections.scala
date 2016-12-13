package com.axa.dil.encoders

import frameless.Injection
import org.joda.time.DateTime

/**
  *
  */
object Injections {
  implicit val dateTimeToLongInjection = new Injection[DateTime, Long] {
    override def apply(d: DateTime): Long = d.getMillis

    override def invert(l: Long): DateTime = new DateTime(l)
  }
}
