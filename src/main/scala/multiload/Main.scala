package multiload

object Main {

  val parser = new scopt.OptionParser[Config]("multiload") {
    head("multiload", "0.1")

    opt[String]('f', "files").required().action( (x, c) =>
      c.copy(jdbc = x) ).text("file path to files to load from. * and ** glob matching supported.")

    opt[String]('j', "jdbc").required().action( (x, c) =>
      c.copy(jdbc = x) ).text("jdbc url of the database to load into")

    opt[Unit]('v', "verbose").action( (_, c) =>
      c.copy(verbose = true) ).text("verbose output")

    opt[Unit]('q', "quiet").action( (_, c) =>
      c.copy(verbose = true) ).text("silent output")

    help("help").text("prints this usage text")

    note("some notes.")

  }

  def main(args: Array[String]): Unit = {
    // parser.parse returns Option[C]
    parser.parse(args, Config()) match {
      case Some(config) =>
      // do stuff

      case None =>
      // arguments are bad, error message will have been displayed
    }
  }

}

case class Config(files: String = "", jdbc: String = "", verbose: Boolean = false, quiet: Boolean = false)
