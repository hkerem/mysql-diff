import sbt._

class MysqlDiffProject(info: ProjectInfo) extends DefaultProject(info) {
    
    override def mainSourceRoots = ("src" / "main" / "scala") +++ ("scala-misc" / "src")
    
}

// vim: set ts=4 sw=4 et:
