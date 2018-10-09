package org.apache.spark

import java.io.{File, IOException}
import java.util.UUID

import org.apache.spark.util.Utils

object SparkUtils {
  private val shutdownDeletePaths = new scala.collection.mutable.HashSet[String]()

  def clearLocalRootDirs(): Unit = {
    Utils.clearLocalRootDirs()
  }

  // Register the path to be deleted via shutdown hook
  def registerShutdownDeleteDir(file: File) {
    val absolutePath = file.getAbsolutePath
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths += absolutePath
    }
  }


  /**
    * Create a directory inside the given parent directory.
    * The directory is guaranteed to be newly created, and is not marked for automatic
    * deletion.
    */
  def createDirectory(root: String): File = {
    var attempts = 0
    val maxAttempts = 10
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException(
          s"Failed to create a temp directory (under $root) after $maxAttempts")
      }
      try {
        dir = new File(root, "spark-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch { case _: SecurityException => dir = null; }
    }
    dir
  }

  /**
    * Create a temporary directory inside the given parent directory.
    * The directory will be automatically deleted when the VM shuts down.
    */
  def createTempDir(root: String = System.getProperty("java.io.tmpdir")): File = {
    val dir = createDirectory(root)
    registerShutdownDeleteDir(dir)
    dir
  }

}
