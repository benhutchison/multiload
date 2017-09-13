package multiload

import java.io.{File, IOException}
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import scala.collection.mutable.ListBuffer

object Main {


  val parser = new scopt.OptionParser[Config]("multiload") {
    head("multiload", "0.1")

    opt[String]('f', "files").required().action( (x, c) =>
      c.copy(files = x) ).text("file path to files to load from. * and ** glob matching supported.")

    opt[String]('j', "jdbc").required().action( (x, c) =>
      c.copy(jdbc = x) ).text("jdbc url of the database to load into")

    opt[String]('u', "username").required().action( (x, c) =>
      c.copy(user = x) ).text("database username")

    opt[String]('p', "password").required().action( (x, c) =>
      c.copy(password = x) ).text("database username")


    opt[Seq[String]]('c', "column").
      action((x, c) => c.copy(columns = c.columns :+ ColumnSpec.fromSeq(x).right.get)).
      validate( x => ColumnSpec.fromSeq(x) match {
        case Right(_) => success
        case Left(msg) => failure(msg)
      }).valueName(ColumnSpec.ColInputDescription).
      text("Specifies the name or index of a CSV column to be loaded." +
        "To refer to input columns by Name, the CSV must have a header row." +
        "Indexes are specified using codes COL0, COL1.. . " +
        "If a second arg is provided, the input will be loaded into the database column with that name. " +
        "If a 3rd argumwent ")

    opt[Unit]('v', "verbose").action( (_, c) =>
      c.copy(verbose = true) ).text("verbose output")

    help("help").text("prints this usage text")

    cmd("insert").action( (_, c) => c.copy(mode = Insert) ).
      text("insert rows into an existing table. Either column mapping are defined for each column, or " +
        "the input CSV has a header row, and each column exists in the target table.")

    note("some notes.")

  }

  def main(args: Array[String]): Unit = {
    // parser.parse returns Option[C]
    parser.parse(args, Config()) match {
      case Some(config) =>
        println(s"valid config: $config")
        val filePaths = filesInGlobPath(config.files)
        println(s"Files in path: ${filePaths.mkString(",")}")

      case None =>
      // arguments are bad, error message will have been displayed
    }
  }

  def filesInGlobPath(path: String): List[Path] = {
    val preglob = path.takeWhile(_ != '*')
    val preglobFile = new File(preglob)
    val baseDir = if (preglobFile.isDirectory)
        preglobFile.toPath
      else if (preglobFile.getParentFile != null)
        preglobFile.getParentFile.toPath
      else
        Paths.get(".")

    val matchedFiles = ListBuffer[Path]()
    val matcher = FileSystems.getDefault.getPathMatcher(s"glob:$path")
    Files.walkFileTree(baseDir, new SimpleFileVisitor[Path]() {
      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        if (matcher.matches(file)) {
          matchedFiles += file
        }
        FileVisitResult.CONTINUE
      }
    })
    matchedFiles.toList
  }

}

case class ColumnSpec(input: InputCol, optOutputCol: Option[String] = None, sqlType: SqlType = StringType)
object ColumnSpec {
  val ColInputDescription = "<Column Name>|<COLn> [Name in table [SQL data type]]"

  val RegexTableName = "([0-9a-zA-Z$_]+)".r

  def fromSeq(s: Seq[String]): Either[String, ColumnSpec] = s match {
    case Seq(inputCol) =>
      InputCol.parse(inputCol).map(new ColumnSpec(_))
    case Seq(inputCol, RegexTableName(outputCol)) =>
      InputCol.parse(inputCol).map(new ColumnSpec(_, Some(outputCol)))
    case Seq(inputCol, RegexTableName(outputCol), typStr) =>
      InputCol.parse(inputCol).flatMap(ic =>
        SqlType.parse(typStr).map(sqlType => new ColumnSpec(ic, Some(outputCol), sqlType)))
    case _ => Left(s"Invalid column spec '${s.mkString(" ")}' doesnt match: $ColInputDescription")
  }
}

sealed trait SqlType
case object StringType extends SqlType
object SqlType {

  def parse(s: String): Either[String, SqlType] = s match {
    case "string" => Right(StringType)
    case other => Left(s"Invalid data type '$other'")
  }
}

sealed trait InputCol
case class ColIndex(index: Int) extends InputCol
case class ColName(name: String) extends InputCol
object InputCol {
  import ColumnSpec.RegexTableName
  val RegexColumnIndex = "COL(\\d+)".r


  def parse(raw: String): Either[String, InputCol] = raw match {
    case RegexTableName(name) => Right(ColName(name))
    case RegexColumnIndex(index) => Right(ColIndex(index.toInt))
    case _ => Left(s"Invalid input column identifier doesn't match patterns for name ${RegexTableName} " +
      s"or column ${RegexColumnIndex}")
  }

}


sealed trait Mode
case object Insert extends Mode

case class Config(mode: Mode = Insert, files: String = "",
                  jdbc: String = "", user: String = "", password: String = "",
                  columns: Seq[ColumnSpec] = Seq.empty,
                  verbose: Boolean = false, quiet: Boolean = false)
