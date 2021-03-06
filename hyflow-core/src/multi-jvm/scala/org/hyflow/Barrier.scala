/**
 *  Copyright (C) 2009-2012 Typesafe Inc. <http://www.typesafe.com>
 */

package org.hyflow

trait Barrier {
  def await() = { enter(); leave() }

  def apply(body: ⇒ Unit) {
    enter()
    body
    leave()
  }

  def enter(): Unit

  def leave(): Unit
}
