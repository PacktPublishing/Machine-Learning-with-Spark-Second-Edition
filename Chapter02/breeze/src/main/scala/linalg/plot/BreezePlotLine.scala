package linalg.plot
import breeze.linalg._
import breeze.plot._

object BreezePlotLine {
  def main(args: Array[String]): Unit = {

    val f = Figure()
    val p = f.subplot(0)
    val x = DenseVector(0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8)
    val y = DenseVector(1.1, 2.1, 0.5, 1.0, 3.0, 1.1, 0.0, 0.5,2.5)
    p += plot(x,  y)
    p.xlabel = "x axis"
    p.ylabel = "y axis"
    f.saveas("lines-graph.png")
  }
}