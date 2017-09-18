package multiload

import java.io._
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.sql.{Connection, DriverManager}

import scala.collection.mutable.ListBuffer
import net.tixxit.delimited._

import scala.util.Try

import cats._
import cats.implicits._

import mouse.all._

object Main {

  val ModeInsert = "insert"

  val BatchSize = 1000


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
        "To refer to mapping columns by Name, the CSV must have a header row." +
        "Indexes are specified using codes COL0, COL1.. . " +
        "If a second arg is provided, the mapping will be loaded into the database column with that name. " +
        "If a 3rd argument ")

    opt[Unit]('h', "header").action((_, c) =>
      c.copy(verbose = true) ).text("Interpret first row  of mapping CSV files as header")

    opt[Unit]('v', "verbose").action( (_, c) =>
      c.copy(verbose = true) ).text("verbose error output")

    help("help").text("prints this usage text")

    cmd(ModeInsert).action( (_, c) => c.copy(mode = Insert) ).
      text("insert rows into an existing table. Either column mapping are defined for each non-nullable column, or " +
        "the mapping CSV has a header row, and each column exists in the target table.").
      children(
        opt[String]("tablename").required().action((x, c) =>
          c.copy(tablename = x) ).text("Existing table to insert data into"),
        opt[Boolean]("truncate").action( (x, c) =>
          c.copy(truncate = x) ).text("truncate table before insert data, default=false."),
      )

    note("some notes.")

  }

  def doInsertBatch(conn: Connection, columnMappings: Seq[ColumnSpec],
                    batch: Seq[Either[DelimitedError, Row]], tablename: String, colSql: String): Either[String, JobProgress] = {

    val st = conn.createStatement()
    val result = for {
      values <- batch.toList.traverse((tryRow: Either[DelimitedError, Row]) => tryRow.map(_.map(field => s"'$field'")).left.map(_.toString))
      _ <- Try(st.addBatch(s"Insert into $tablename ($colSql) values(${values.mkString(", ")})")).toEither.left.map(_.toString)
      okCount <- Try(st.executeBatch()).toEither.left.map(_.toString)
    } yield (JobProgress(okCount.sum, 0))
    st.close()
    result
  }

  def columnListSql(conn: Connection, config: Config): String = {
    config.columns.map {
      case IndexToName(index, nameOut, _) => nameOut
      case NameToName(nameIn, nameOut, _) => nameOut
    }.mkString(" ,")
  }

  def getOrderedColumns(conn: Connection, tablename: String): Either[String, IndexedSeq[String]] = {
    val colBuffer = new ListBuffer[(Int, String)]
    Try {
      val rs = conn.getMetaData().getColumns(null, null, tablename, null)
      while(rs.next()) {
        val colIdx = rs.getInt("ORDINAL_POSITION")
        val colName = rs.getString("COLUMN_NAME")
        colBuffer += (colIdx -> colName)
      }
      //ensure the columns are sorted so they'll line up against CSV
      val (idxs, names) = colBuffer.toSeq.sorted.unzip
      //Sanity check: ensure all indexes are present and exactly once
      require(idxs.toList == 1.to(idxs.size).toList)
      names.toIndexedSeq
    }.toEither.left.map(_.toString)
  }

  def doInsertFile(conn: Connection, config: Config, path: Path): Either[String, JobProgress] = {
    val (rowIter, releaseHandle) = rowsFrom(path)
    val columnListSql1 = columnListSql(conn, config)

    for {
      optHeaders <- (config.header).option(rowIter.next().map(_.toIndexedSeq.zipWithIndex).left.map(_.toString())).sequence
      optHeaders1: Option[IndexedSeq[(String, Int)]] = optHeaders
      columnMappings <- (config.columns.nonEmpty, optHeaders1.isDefined) match {
        case (true, true) =>
          convertInputNamesToIndices(config.columns, optHeaders1)
        case (false, true) =>
          optHeaders1.toRight("Impossible").map(_.map {case(c, i) => IndexToName(i, c)})
        case (true, false) =>
          val nameMappings = config.columns.filter(_.isInstanceOf[NameToName])
          if (nameMappings.nonEmpty)
            Left(s"Column name mapping defined but no header")
          else
            Right(config.columns)
        case (false, false) =>
          Left(s"Error processing file $path. " +
            s"Either at least one columns mapping (-c option) or an mapping file header (-h flag) must be provided.")
      }
      batchResults <- rowIter.grouped(BatchSize).toStream.traverse(
        batch => doInsertBatch(conn, columnMappings, batch, config.tablename, columnListSql1))
    } yield JobProgress(batchResults.map(_.success).sum, 0)
  }

  def convertInputNamesToIndices(colSpecs: Seq[ColumnSpec],
                                 optHeader: Option[IndexedSeq[(String, Int)]]): Either[String, List[IndexToName]] = {

    val namesToIndices = optHeader.map(_.toMap).toRight(s"Column mapping specifies named input but no input column header names.")
    colSpecs.toList.traverse {
      case NameToName(in, out, typ) => namesToIndices.flatMap(m =>
        m.get(in).toRight(s"Input column $in not found in: ${m.keys.mkString(", ")}")).map(i =>
        IndexToName(i, out, typ)
      )
      case in: IndexToName => Right(in)
    }
  }

  def main(args: Array[String]): Unit = {
    // parser.parse returns Option[C]
    parser.parse(args, Config()) match {
      case Some(config) =>
        println(s"valid config: $config")

        val filePaths = filesInGlobPath(config.files)
        println(s"Files in path: ${filePaths.mkString(",")}")

        Class.forName("org.postgresql.Driver")
        val conn = DriverManager.getConnection(config.jdbc, config.user, config.password)
        conn.setAutoCommit(false)
        println(s"Connected OK to: ${config.jdbc} as user: ${config.user}")

        if (config.mode == ModeInsert)
          for {
            outcomes <- filePaths.traverse(doInsertFile(conn, config, _))
          } yield (outcomes)

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

  def rowsFrom(csvFile: Path): (Iterator[Either[DelimitedError, Row]], Closeable) = {
    val parser: DelimitedParser = DelimitedParser(DelimitedFormat.Guess)
    val fr = new FileReader(csvFile.toFile)
    val reader: Reader = new BufferedReader(fr)
    (parser.parseReader(reader), fr)
  }

}



case class JobProgress(success: Int, fail: Int) {

  def total = success + fail
}

sealed trait ColumnSpec
case class IndexToName(index: Int, name: String, sqlType: SqlType = StringType) extends ColumnSpec
case class NameToName(name: String, outName: String, sqlType: SqlType = StringType) extends ColumnSpec
object ColumnSpec {
  val ColInputDescription = "<mapping name> [<output name>] [<datatype>] | <'COLn' mapping index> <output name> [data type]]"

  val RegexTableName = "([0-9a-zA-Z$_]+)".r
  val RegexColumnIndex = "COL(\\d+)".r

  def fromSeq(s: Seq[String]): Either[String, ColumnSpec] = s match {
    case Seq(RegexTableName(n)) =>
      Right(new NameToName(n, n))
    case Seq(RegexColumnIndex(i), RegexTableName(n)) =>
      Right(new IndexToName(i.toInt, n))
    case Seq(RegexTableName(n), RegexTableName(o), typStr) =>
        SqlType.parse(typStr).map(sqlType => new NameToName(n, o, sqlType))
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


sealed trait Mode
case object Insert extends Mode

case class Config(mode: Mode = Insert, files: String = "",
                  jdbc: String = "", user: String = "", password: String = "",
                  tablename: String = "", truncate: Boolean = false,
                  columns: Seq[ColumnSpec] = Seq.empty,
                  header: Boolean = false,
                  verbose: Boolean = false, quiet: Boolean = false)
