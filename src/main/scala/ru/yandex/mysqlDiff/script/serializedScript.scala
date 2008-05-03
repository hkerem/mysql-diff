package ru.yandex.mysqlDiff.script

case class SerializedScript(lines: Seq[ScriptLine]) {
    def ++(script: SerializedScript) = new SerializedScript(lines ++ script.lines)
    
    def indent(delta: Int) = new SerializedScript(lines.map(_.indent(delta)))
    
    def mkString(sep: String) = lines.map(_.string).mkString(sep)
    
    override def toString = mkString(" ")
}

case class ScriptLine(string: String, indent: Int) {
    def this(string: String) = this(string, 0)

    def indent(delta: Int): ScriptLine = new ScriptLine(string, indent + delta)
}

object SerializedScript {
    def empty = new SerializedScript(Nil)
    
    //implicit def apply(lines: Seq[ScriptLine]): SerializedScript = new SerializedScript(lines)
    implicit def scriptFromStrings(lines: Seq[String]): SerializedScript = apply(lines.map(ScriptLine(_, 0)))
    implicit def scriptFromSingleString(line: String): SerializedScript = scriptFromStrings(List(line))
    
    implicit def lines(script: SerializedScript): Seq[ScriptLine] = script.lines
    
    implicit def line(line: String) = new ScriptLine(line, 0)
}

// vim: set ts=4 sw=4 et:
