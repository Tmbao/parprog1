package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    @tailrec
    def balanceLoop(acc: Int, index: Int, chars: Array[Char]): Boolean =
      if (acc < 0) false
      else if (index == chars.length)
        if (acc == 0) true
        else false
      else balanceLoop(
        if (chars(index) == '(') acc + 1
        else if (chars(index) == ')') acc - 1
        else acc,
        index + 1, chars)

    balanceLoop(0, 0, chars)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def getValue(char: Char): Int =
      if (char == '(') 1
      else if (char == ')') -1
      else 0

    @tailrec
    def traverse(idx: Int, until: Int, acc: Int, nadir: Int): (Int, Int) = {
      if (idx == until) (acc, nadir)
      else {
        val newAcc = acc + getValue(chars(idx))
        val newNadir = math.min(nadir, newAcc)
        traverse(idx + 1, until, newAcc, newNadir)
      }
    }

    def reduce(from: Int, until: Int): (Int, Int) = {
      if (until - from <= threshold)
        traverse(from, until, 0, 0)
      else {
        val mid = (from + until) / 2
        val ((leftAcc, leftNadir), (rightAcc, rightNadir)) = parallel(
          reduce(from, mid),
          reduce(mid, until)
        )
        (leftAcc + rightAcc, math.min(leftNadir, leftAcc + rightNadir))
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
