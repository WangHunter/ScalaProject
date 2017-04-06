package scala

/**
  * Created by Administrator on 2017/3/17.
  */
object HelloWorld {
  def main(args: Array[String]): Unit = {
//    println("Hello World")

    val s = "hello"
    var sum = 0
    for(i <- 0 until s.length) sum += s(i)

    for (i <- 0 to 10 reverse)print(i)

//    box("hello")
//    println(signum(-7))

  }

  def box(s: String): Unit = {
    var border = "-"* s.length + "--\n"
    println(border + "|" + s + "|\n" + border)
  }

  def signum(s: Int)={
    if (s == 0) 0 else if (s > 0) 1 else -1
  }

  val kafkaStream =
}
