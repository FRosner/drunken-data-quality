package de.frosner.ddq.reporters

import java.io.PrintStream

trait PrintStreamReporter extends Reporter {

  val stream: PrintStream

}
