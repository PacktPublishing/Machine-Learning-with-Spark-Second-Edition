package linalg.plot
import breeze.linalg._
import breeze.plot._

object BreezePlotGaussian {
  def main(args: Array[String]): Unit = {
    val f = Figure()
    val p = f.subplot(2, 1, 1)
    val g = breeze.stats.distributions.Gaussian(0, 1)
    p += hist(g.sample(100000), 100)
    p.title = "A normal distribution"
    f.saveas("plot-gaussian-100000.png")
  }
}