package linalg.fields

/**
  * Created by manpreet.singh on 27/01/16.
  */
trait GF2 {
  def + (that: GF2): GF2
  def * (that: GF2): GF2

  def / (that: GF2) = that match {
    case Zero => throw new IllegalArgumentException("Div by 0")
    case _ => this
  }
}

object Zero extends GF2 {
  override def toString = "Zero"
  def + (that: GF2) = that
  def * (that: GF2) = this
}

object One extends GF2 {
  override def toString = "One"
  def + (that: GF2) = that match { case One => Zero ; case _ => this }
  def * (that: GF2) = that match { case One => this ; case _ => that }
}