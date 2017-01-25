import java.io.File
import java.io.InputStream
import java.io.FileInputStream
import java.util._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

object hdfsUtility {


	def getFS(): FileSystem = {
		var conf = new Configuration
		conf.set("fs.default.name", "hdfs://localhost:9000")
		FileSystem.newInstance(conf)
	}

/**
   * Function to get files recursively
   *
   * @param filePath Path to a specific folder
   * @param fs FileSystem
   * @return All paths found recursively
   */
  def getAllFilePath(path: String): Seq[String] = {
  	val fs=getFS
    var fileList = Seq.empty[String]
    fs.listStatus(new Path(path))
      .foreach { fileStat =>
          fileList = fileList.+:(fileStat.getPath().toString())
      }
    return fileList
  }


	/*
	 * create a new folder 
	 * @param ip of name node
	 * @param port of name node
	 * @param folderPath to create new folder
	 */
	def mkdirs(folderPath: String): String = {
		try {
			val fs = getFS
			var path = new Path(folderPath)
			if (!fs.exists(path)) {
				fs.mkdirs(path)
				"T:Directory Created Successfully"
			} else
				"F:Directory already exists : "
		} catch {
			case ex: Exception => {
				"F: makde DIR exception" + ex.getMessage()
			}
		}
	}

	def download(hdfsPath: String, localPath: String): String = {
		try {
			val fs = getFS
			val hdfs = new Path(hdfsPath)
			val local = new Path(localPath)
			fs.copyToLocalFile(hdfs, local)
			"Downloaded"
		} catch {
			case ex: Exception => {
				"F: Download Exception " + ex.getMessage()
			}
		}
	}

	def upload(hdfsPath: String, localPath: String): String = {
		try {
			val fs = getFS
			val hdfs = new Path(hdfsPath)
			val local = new Path(localPath)
			fs.copyFromLocalFile(local, hdfs)
			"Uploaded"
		} catch {
			case ex: Exception => {
				"F: Exception during uploading ..... " + ex.getMessage()
			}
		}
	}
	
	/**	 * remove directories recursively
	 *  @param path to directory to be removed
	 *  @return true or false
	 */
	def rmr(path: String): Boolean = {
		var fs = getFS
		if (fs.exists(new Path(path))) {
			fs.delete(new Path(path), true)
		}
		false
	}

	/**
	 * remove a file
	 *  @param filename fully qualified file name to be removed
	 *  @return true or false
	 */
	def removeFile(filename: String): Boolean = {
		var fileSystem = getFS
		val path = new Path(filename)
		var deleted = false
		deleted = fileSystem.delete(path, true)
		fileSystem.close()
		deleted
	}


}
