package ru.yandex.mysqlDiff.ant

import org.apache.tools.ant._
import java.io.File
import scalax.io.Implicits._

import diff._

class DiffTask extends Task {
    import Environment.defaultContext._

    private var from: String = _
    private var to: String = _
    private var output: File = _
    
    def setFrom(from: String) { this.from = from; }
    def setTo(to: String) { this.to = to; }
    def setOutput(output: File) { this.output = output; }
    
    override def execute() {
        if (from eq null) throw new IllegalStateException("required parameter 'from' not set")
        if (to eq null) throw new IllegalStateException("required parameter 'to' not set")
        if (output eq null) throw new IllegalStateException("required parameter 'output' not set")
        
        val fromModel = Utils.getModelFromArgsLine(from)
        val toModel = Utils.getModelFromArgsLine(to)
        
        val dbDiff = diffMaker.compareDatabases(fromModel, toModel)
        val script = DiffSerializer.serialize(fromModel, toModel, dbDiff)
        
        log("Writing diff to " + output)
        output.write(script)
    }

} //~

// vim: set ts=4 sw=4 et:
