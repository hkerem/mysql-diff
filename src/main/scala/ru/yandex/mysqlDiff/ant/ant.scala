package ru.yandex.mysqlDiff.ant

import org.apache.tools.ant._
import java.io.File
import scalax.io.Implicits._

import diff._

/**
 * Ant task to compare databases.
 */
class DiffTask extends Task {
    // XXX: make context dependent on DB vendor
    import Environment.defaultContext._

    private var from: String = _
    private var to: String = _
    private var output: File = _
    
    /** Soruce database, either file or JDBC URL */
    def setFrom(from: String) { this.from = from; }
    /** Target database, either file or JDBC URL */
    def setTo(to: String) { this.to = to; }
    /** Where to save output */
    def setOutput(output: File) { this.output = output; }
    
    override def execute() {
        if (from eq null) throw new IllegalStateException("required parameter 'from' not set")
        if (to eq null) throw new IllegalStateException("required parameter 'to' not set")
        if (output eq null) throw new IllegalStateException("required parameter 'output' not set")
        
        val fromModel = utils.getModelFromArgsLine(from)
        val toModel = utils.getModelFromArgsLine(to)
        
        val dbDiff = diffMaker.compareDatabases(fromModel, toModel)
        val script = diffSerializer.serialize(fromModel, toModel, dbDiff)
        
        log("Writing diff to " + output)
        output.write(script)
    }

} //~

// vim: set ts=4 sw=4 et:
