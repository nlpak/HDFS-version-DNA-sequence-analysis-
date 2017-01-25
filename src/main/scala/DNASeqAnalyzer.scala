/* 
 * Copyright (c) 2015-2016 TU Delft, The Netherlands.
 * All rights reserved.
 * 
 * You can redistribute this file and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the
 * Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This file is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * Authors: Hamid Mushtaq
 *
*/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level

import sys.process._
import scala.sys.process.Process

import java.io._
import java.nio.file._
import org.apache.spark.HashPartitioner
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Calendar

import tudelft.utils.ChromosomeRange
import tudelft.utils.DictParser
import tudelft.utils.Configuration
import tudelft.utils.SAMRecordIterator
import org.apache.spark.storage.StorageLevel
import htsjdk.samtools._
import scala.io.Source

object DNASeqAnalyzer {
  final val MemString = "-Xmx5120m"
  final val RefFileName = "ucsc.hg19.fasta"
  final val SnpFileName = "dbsnp_138.hg19.vcf"
  final val ExomeFileName = "gcat_set_025.bed"
  //////////////////////////////////////////////////////////////////////////////

// helping function to replace X and Y with higher chromosome number that will help to sort 
  def getChrNbr(chrX: String): Int = {
    var chrNb = 35
    var str = chrX.substring(3, chrX.size)
    if (str.equalsIgnoreCase("x")) {
      chrNb = 25
    } else if (str.equalsIgnoreCase("y")) {
      chrNb = 26
    } else {
      chrNb = str.toInt
    }

    return chrNb
  }

  def getTimeStamp(): String =
    {
      return "[" + new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime()) + "] "
    }

    def bwaRun(x2: String, config: Configuration): Array[(Int, SAMRecord)] =
    {

       // getting file and folders path from congig object 
      val refFolder = config.getRefFolder
      val tmpFolder = config.getTmpFolder
      val toolsFolder = config.getToolsFolder
      val x = x2.split("/")(x2.split("/").size-1) 
      val hdfUri = config.getNameNodeUrl
      val outputFolder = config.getOutputFolder //hdfs
      val hdf_bwaLog_dir = hdfUri+outputFolder+"log/bwa/" // log file path 
    
      // downloading input chunk for current bwaRun from hdfs  into tmp folder   
      hdfsUtility.download(x2, tmpFolder)
           
      val numOfThreads = config.getNumThreads
      val outputFile = x.substring(x.lastIndexOf("/") + 1).split("\\.")(0)
      val logFile = tmpFolder + "log/bwa/" + s"$outputFile" + "_log.txt"
      // creating loger file in tmp folder, will append this file in hdfs log/bwa
      val bwaLogger = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logFile), "UTF-8"))
     
      bwaLogger.write("--------------------------------------------------- -----------------\n")
      bwaLogger.flush()
      bwaLogger.write(getTimeStamp() + "bwaRun called for Chunk : " + x+" \n")
      
      bwaLogger.write(getTimeStamp() + "downloadibng data: " + x+" \n")
      // making file paths for input file downloaded in tmp folder from hdfs 
      val fqInputFile = s"$tmpFolder"+s"$x"
      // path for reference file 
      val refFilePath = s"$refFolder"+s"$RefFileName" 
      val bwaMemCmdPath = s"$toolsFolder"+"bwa"
      //  preparing bwa mem command strings, with respective input, output and reference files paths 
      val bwaMemCmd = (s"$bwaMemCmdPath mem $refFilePath -p -t $numOfThreads $fqInputFile" #> new File(s"$tmpFolder/$outputFile"))
      bwaLogger.write(getTimeStamp() + "bwa mem command executed : " + bwaMemCmd+" \n")
      // executing bwa mem command 
      bwaMemCmd.!

      val bwaKeyVAlInputFile = s"$tmpFolder"+s"$outputFile"
      val bwaKeyValues = new BWAKeyValues(s"$bwaKeyVAlInputFile")
      val count = bwaKeyValues.parseSam()
      val kvPairs: Array[(Int, SAMRecord)] = bwaKeyValues.getKeyValuePairs()

      bwaLogger.write(getTimeStamp() + " SAMstream counts " + count + " records \n"); 
      bwaLogger.write(getTimeStamp() + " Deleting tmp Files ...\n")
      bwaLogger.flush()
      bwaLogger.close()
      // uploading log file into hdfs 
      hdfsUtility.upload(hdf_bwaLog_dir,logFile) 
      // deleting files from tmp folder 
      new File(s"$tmpFolder/$outputFile").delete()
      new File(s"$tmpFolder/$x").delete()
      new File(s"$logFile").delete()
      // new File(toolsFolder).delete()
      
      // returning Array <Charnumber, SAM Record>  
      return kvPairs // Replace this with return kvPairs
    
}


  def writeToBAM(fileName: String, samRecordsSorted: Array[SAMRecord], config: Configuration): ChromosomeRange =
    {
      println("Writting File for :: " + fileName)
      val header = new SAMFileHeader()
      header.setSequenceDictionary(config.getDict())
      val outHeader = header.clone()
      outHeader.setSortOrder(SAMFileHeader.SortOrder.coordinate);
      val factory = new SAMFileWriterFactory();
      val writer = factory.makeBAMWriter(outHeader, true, new File(fileName));

      val r = new ChromosomeRange()
      val input = new SAMRecordIterator(samRecordsSorted, header, r)
      while (input.hasNext()) {
        val sam = input.next()
        writer.addAlignment(sam);
      }
      writer.close();

      return r
    }

 def variantCall(chrRegion: Int, samRecordsSorted: Array[(Int, Int, SAMRecord)], config: Configuration): Array[(Int, (Int, String))] =
    {
      // getting file and folders path from congig object 
      val tmpFolder = config.getTmpFolder
      val toolsFolder = config.getToolsFolder 
      val refFolder = config.getRefFolder
      val numOfThreads = config.getNumThreads

      // to upload on hdfs 
      val hdfUri = config.getNameNodeUrl
      val outputFolder = config.getOutputFolder //hdfs
      val hdf_vcLog_dir = hdfUri+outputFolder+"log/vc/"
      val hdf_output_dir = hdfUri+outputFolder  

      // new log file 
      val logFile = tmpFolder + "log/vc/region" + s"$chrRegion" + "_log.txt"

      val variantLogger = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logFile), "UTF-8"))
      variantLogger.write("--------------------------------------------------- -----------------\n")
      variantLogger.flush()

      variantLogger.write(getTimeStamp() + " variantCall is called for the Region : " + chrRegion + " \n") 
      
      variantLogger.write(getTimeStamp() + " Number of Reads for that Region : " + samRecordsSorted.size + " \n")
      
       // Array[(chr number, ReadPostion, SAM REcord)]
      // sorting the array, first on SAM record read position, then on Chr number, all a chr number will appear first, and records having same chr number will be sorted on read position	
      val sorted = samRecordsSorted.sortBy(_._2).sortBy(_._1).map(_._3)

      val fileName = s"$tmpFolder" + s"region$chrRegion" + "-p1.bam" //s"$tmpFolder/region$chrRegion"+"-p1.bam"
      val chrRange = writeToBAM(fileName, sorted, config)
      variantLogger.write(getTimeStamp() + " Writting BAM file for the Region : " + chrRegion + " \n")

      // STEP 1
      // Picard preprocessing
      variantLogger.write(getTimeStamp() + " step 1: Picard preprocessing started ... : " + " \n")
      
      // preparing java command strings, with respective input, output and reference paths 
      val outputPicard1 = s"$tmpFolder" + s"region$chrRegion" + "-p2.bam"
      val picradCmd1 = (s"java $MemString -jar $toolsFolder" + s"CleanSam.jar INPUT=$fileName OUTPUT=$outputPicard1")
      val outputPicard2 = s"$tmpFolder" + s"region$chrRegion" + "-p3.bam"
      val outputPicard3 = s"$tmpFolder" + s"region$chrRegion" + ".bam"
      val metrics_file = s"$tmpFolder" + s"region$chrRegion" + "-p3-metrics.txt" // regionX-p3-metrics.txt
      val picradCmd2 = (s"java $MemString -jar $toolsFolder" + s"MarkDuplicates.jar INPUT=$outputPicard1 OUTPUT=$outputPicard2 METRICS_FILE=$metrics_file")
      val picardCmd3 = (s"java $MemString -jar $toolsFolder" + s"AddOrReplaceReadGroups.jar INPUT=$outputPicard2 OUTPUT=$outputPicard3 RGID=GROUP1 RGLB=LIB1 RGPL=ILLUMINA RGPU=UNIT1 RGSM=SAMPLE1")
      val picardCmd4 = (s"java $MemString -jar $toolsFolder" + s"BuildBamIndex.jar INPUT=$outputPicard3")


      variantLogger.write(getTimeStamp() + " Executing command ... : "+picradCmd1 + " \n")
      variantLogger.write(getTimeStamp() + " Executing command ... : "+picradCmd2 + " \n")
      variantLogger.write(getTimeStamp() + " Executing command ... : "+picardCmd3 + " \n")
      variantLogger.write(getTimeStamp() + " Executing command ... : "+picardCmd4 + " \n")
    
      // executing commands in order to produce intermediate files that will be used in next steps 
      picradCmd1.!
      picradCmd2.!
      picardCmd3.!
      picardCmd4.!

      // Deleting tmp files for Picard preprocessing
      new File(fileName).delete
      new File(outputPicard1).delete
      new File(outputPicard2).delete
      new File(metrics_file).delete
      // writting logs 
      variantLogger.write(getTimeStamp() + " Deleting tmp files for Picard preprocessing ... : " + " \n")
    
      
      // STEP 2 
      // Generating BED Files 
      variantLogger.write(getTimeStamp() + " step 2: BED Files Generation started ... : " + " \n")
      
      val tmpBed = new File(s"$tmpFolder" + s"tmp$chrRegion" + ".bed")
      val outPutBed = s"$tmpFolder" + "bed" + s"$chrRegion" + ".bed"
      chrRange.writeToBedRegionFile(tmpBed.getAbsolutePath())
      
      val ExomeFileNamePath = s"$refFolder"+s"$ExomeFileName" 
      val bedtoolPath = s"$toolsFolder" + "bedtools"
      val badRegionCmd = (s"$bedtoolPath intersect -a $ExomeFileNamePath -b $tmpBed -header" #> new File(outPutBed))
      
      // executing bed tool cmd 
      badRegionCmd.!
      
      variantLogger.write(getTimeStamp() + " Executing command ... : "+badRegionCmd + " \n")
      
      variantLogger.write(getTimeStamp() + " Deleting tmp files for BED preprocessing ... : " + " \n")
      tmpBed.delete()
       
      // step 3:
      // Indel Realignment 
      variantLogger.write(getTimeStamp() + " step 3: Indel Realignment started ... : " + " \n")
 

      // preparing paths for tmp files and output files generating Indel Realignment
      val intervalsFile = s"$tmpFolder" + "region" + s"$chrRegion" + ".intervals"
      val refFilePath = s"$refFolder"+s"$RefFileName"

      val IndelCmd = (s"java $MemString -jar $toolsFolder" + s"GenomeAnalysisTK.jar -T RealignerTargetCreator -nt $numOfThreads -R " + s"$refFilePath -I $outputPicard3 -o $intervalsFile -L $outPutBed")
      val outputTK = s"$tmpFolder" + s"region$chrRegion" + "-2.bam" // tmpFolder/regionX-2.bam
      val tkCmd = (s"java $MemString -jar $toolsFolder" + s"GenomeAnalysisTK.jar -T IndelRealigner -R " + s"$refFilePath  -I $outputPicard3 -targetIntervals $intervalsFile -o $outputTK -L $outPutBed")

      // preparing java command strings for Indel Realignment, with respective input, output and reference paths 
      variantLogger.write(getTimeStamp() + " Executing command ... : "+IndelCmd + " \n")
      variantLogger.write(getTimeStamp() + " Executing command ... : "+tkCmd + " \n")
      
      // executing Indel Realignment commands 
      IndelCmd.!
      tkCmd.!

      variantLogger.write(getTimeStamp() + " Deleting tmp files generated during Indel Realignment ...  " + " \n")
      val baiFile = s"$tmpFolder" + s"region$chrRegion" + ".bai" // RegionX.bai
      new File(outputPicard3).delete()
      new File(intervalsFile).delete()
      new File(baiFile).delete()

      // STEP 4...
      // Base quality recalibration 
      
      variantLogger.write(getTimeStamp() + " step 4: Base quality recalibration started ... : " + " \n")
 
      val baseOutputTbl = s"$tmpFolder" + s"region$chrRegion" + ".table"
      val outputBaseQulity = s"$tmpFolder" + s"region$chrRegion" + "-3.bam"

      // preparing java command strings for Indel Realignment, with respective input, output and reference paths 
      val baseQulityCmd1 = (s"java $MemString -jar $toolsFolder" + s"GenomeAnalysisTK.jar -T BaseRecalibrator -nct $numOfThreads -R $refFolder/" + s"$RefFileName -I " + s"$outputTK -o $baseOutputTbl -L $outPutBed --disable_auto_index_creation_and_locking_when_reading_rods -knownSites $refFolder/$SnpFileName")
      val baseQulityCmd2 = (s"java $MemString -jar $toolsFolder" + s"GenomeAnalysisTK.jar -T PrintReads -R $refFolder" + s"$RefFileName -I " + s"$outputTK -o $outputBaseQulity -BQSR $baseOutputTbl -L $outPutBed ")

      variantLogger.write(getTimeStamp() + " Executing command ... : "+baseQulityCmd1 + " \n")
      variantLogger.write(getTimeStamp() + " Executing command ... : "+baseQulityCmd2 + " \n")
      // executing Base quality recalibration Commands 
      baseQulityCmd1.!
      baseQulityCmd2.!

      variantLogger.write(getTimeStamp() + " Deleting tmp files used during Base quality recalibration ...  " + " \n")
      val baiFile2 = s"$tmpFolder" + s"region$chrRegion" + "-2.bai" // RegionX-2.bai
      new File(baseOutputTbl).delete()
      new File(outputTK).delete()
      new File(baiFile2).delete()

      
      //STEP 5
      // Haplotype -> Uses the region bed file
      
      variantLogger.write(getTimeStamp() + " step 5: Base Haplotype started to generate *VCF files for each region... : " + " \n")
      val vcfFile = s"$tmpFolder" + s"region$chrRegion" + ".vcf" //tmpFolder/regionX.vcf 
      
      // preparing java command string
      val haplotypeCmd = (s"java $MemString -jar $toolsFolder" + s"GenomeAnalysisTK.jar -T HaplotypeCaller -nct $numOfThreads -R -R $refFolder/" + s"$RefFileName -I $outputBaseQulity -o $vcfFile  -stand_call_conf 30.0 -stand_emit_conf 30.0 -L $outPutBed --no_cmdline_in_header --disable_auto_index_creation_and_locking_when_reading_rods")

      variantLogger.write(getTimeStamp() + " Executing command ... : "+ haplotypeCmd + " \n")
      // executing haplotype cammand to generate vcf file of that region 
      haplotypeCmd.!

      variantLogger.write(getTimeStamp() + " Deleting tmp files used during VCF file generation ...  " + " \n")
      // deleting tmp files 
      val baiFile3 = s"$tmpFolder" + s"region$chrRegion" + "-3.bai" // RegionX-2.bai
      new File(outputBaseQulity).delete()
      new File(baiFile3).delete()
      new File(outPutBed).delete()
      new File(s"$tmpFolder" + s"region$chrRegion" + ".vcf.idx").delete()
      
      variantLogger.write(getTimeStamp() + " Reading output VCF file : "+vcfFile + " \n")
      
      // reading VCF File of for that region into array 
      val vcfRead = Source.fromFile(vcfFile).getLines.toArray
      // filtering out header lines 
      val lines = vcfRead.filter(!_.startsWith("#"))
      
      variantLogger.write(getTimeStamp() + " Without Header Size of Region_...  "+ chrRegion + " VCF File : "+lines.size + " \n")
      
      // spliting the each record to get chr number, and read position 
      val vcfArray = lines.map { x =>
        val tokens = x.split("\t")
        (getChrNbr(tokens(0)), (tokens(1).toInt, x))  // passing chrnumber: ChrX, Chr4 etc into getChrNbr() to replace X, and Y that will help to sort accordingly
      } 
      
      variantLogger.write(getTimeStamp() + " Completing Variant Call for Region : "+ chrRegion + " \n")
      variantLogger.flush()
      variantLogger.close()
      // upload on hdfs 

  
      hdfsUtility.upload(hdf_output_dir, vcfFile)
      hdfsUtility.upload(hdf_vcLog_dir, logFile) 
      
      new File(logFile).delete
      new File(vcfFile).delete
      //new File(toolsFolder).delete
      
      //  Return those in the form of <Chromsome number, <Chromosome Position, line>>
      return vcfArray 

    }

  def main(args: Array[String]) {
    val config = new Configuration()
    config.initialize()

    val conf = new SparkConf().setAppName("DNASeqAnalyzer")
    // For local mode, include the following two lines
    conf.setMaster("local[" + config.getNumInstances() + "]")
    conf.set("spark.cores.max", config.getNumInstances())
    // conf.set("spark.rdd.compress", "true")

    
    val sc = new SparkContext(conf)

    val brodcastedConfig = sc.broadcast(config)
    val hdfUri =  config.getNameNodeUrl()
    val inputFolder = config.getInputFolder   // hdfs
    val toolsFolder =  config.getToolsFolder()
    val hdfsInput = hdfUri+inputFolder
    val tmpFolder = config.getTmpFolder()
    val outputFolder = config.getOutputFolder //hdfs

    val hdf_output_dir = hdfUri+outputFolder  

    // making directories inside hdfs , Althoug hdfs upload will create directory stcucture by still following convention of Q2 
    // val hdfsLog_dir = outputFolder+"/log"
    val hdf_bwaLog_dir = outputFolder+"/log/bwa"
    val hdf_vcLog_dir = outputFolder+"/log/vc"
    // hdfsUtility.mkdirs(hdfsLog_dir)
    hdfsUtility.mkdirs(hdf_bwaLog_dir)
    hdfsUtility.mkdirs(hdf_vcLog_dir)
    // creating local logs directories 
    val bwaLog_dir = tmpFolder+"log/bwa"
    val vcLog_dir = tmpFolder+"log/vc"
    new File(bwaLog_dir).mkdirs
    new File(vcLog_dir).mkdirs

    // Comment these two lines if you want to see more verbose messages from Spark
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);

    var t0 = System.currentTimeMillis
    // getting list of input fq.gz files from hdfs input folder, library function is defined in hdfsUtilty 
    val filesList = hdfsUtility.getAllFilePath(hdfUri+inputFolder)
    // parallelize the input files list to process these in parallel inside map for bwaRun
    val rdd = sc.parallelize(filesList)   // RDD[String] (chunk0.fq.gz), (chunk1.fq.gz), (chunk2.fq.gz) ...

    // calling bwaRun on each file name  
    val bwaRDD = rdd.map(x => bwaRun(x, brodcastedConfig.value))//.collect()
    
    // bwa run retunred Arrays[(chrNumber, SAM Record)], FlatMAp converted it into RDD[chrNumber, SAM Record]
    val flatRDD = bwaRDD.flatMap(x => x).cache()//.persist(StorageLevel.MEMORY_ONLY_SER) 
    
    // creating a tem rdd of type (Chromosome Number, 1)  
    val tmpRDD = flatRDD.map(x => (x._1, 1))

    // reducing to get total SAM Records for each chormosome number. and sorting in Decending order, to place higer Toal first in Region, that will result in more balanced region devision  
    val countbyChrom = tmpRDD.reduceByKey(_ + _).sortBy(_._2, false)//.persist(StorageLevel.MEMORY_ONLY_SER)


    // creating a Region List of size 4, in each list value will keep the total SAM records for each Region  
    var regionsSum = List(0, 0, 0, 0)

    // helping Funtion, that takes SAM records Count, places these records in regionsSum list into index where minimum Records are present and retunrs Region number where these records are placed 
    def findRegion(count: Int): Int = {
      val availableSlot = regionsSum.zipWithIndex.min._2
      regionsSum = regionsSum.updated(availableSlot, regionsSum(availableSlot) + count)
      return availableSlot + 1
    }

    // calling findRegion for each chromosome number, and getting its region 
    // countbyChrom => (ChrNumber, total_SAM_Records_Count ) , that results into regionRDD => (ChrNumber, Region) 
    val regionRDD = countbyChrom.repartition(1).map(x => (x._1, findRegion(x._2)))

    // joining rigionRDD with RDD returned by bwaRun to have (Region, (ChrNumber, SAM Record)
    val samWithRigon = flatRDD.join(regionRDD).map(x => (x._2._2, (x._1, x._2._1)))

    // getting Read postion for each SAM record objecct
                                                // (region, (chrNb, readPositionm, SAMRecord))         
    val samRecordPosition = samWithRigon.map(x => (x._1, (x._2._1, x._2._2.getAlignmentStart(), x._2._2))) //.cache()//.persist(StorageLevel.MEMORY_ONLY_SER)
   
    //                 (Region, Array(<ChrNumber, ReadPostion, SAM Record>))
    val regionGroupRDD = samRecordPosition.groupByKey.map(x => (x._1, x._2.toArray))

    //  we have 4 regions, and now after group by our RDD count is 4, each having Array of SAM records, its chromostom number and REad postion in that region 
    // calling  variantCall for each Region, passing array, array will be sorted inside variant calling 
    val vcfRDD = regionGroupRDD.map(x => variantCall(x._1, x._2, brodcastedConfig.value)) // .collect()

    // VCF retuned array(<ChrNumber of type Int, (Read postion, SAME REcord ) > ), Flate Map will combine all 4 arrays into single RDD
    val singleVCF = vcfRDD.flatMap(x => x).cache() //.persist(StorageLevel.MEMORY_ONLY_SER)

     // sorting first on on Read postion, then Chr number, second sorting, and appending new line char for each record 
    val sortedVCF = singleVCF.sortBy(_._2._1).sortBy(_._1).map(x => x._2._2.toString + "\n")
    
    val tmpCombineFilePath =  tmpFolder + "combined.vcf"

    val file = scala.tools.nsc.io.Path(tmpCombineFilePath).createFile()

    // saving Combined vcf file 
    sortedVCF.collect().foreach { x =>
      file.appendAll(x)
    }
    // uploading combined vcf File on hdfs 
    hdfsUtility.upload(hdf_output_dir, tmpCombineFilePath)
    // deleting combined vcf file from tmp folder s
    new File(tmpCombineFilePath).delete
    new File(tmpFolder+"log").delete

    val et = (System.currentTimeMillis - t0) / 1000
    println("|Execution time: %d mins %d secs|".format(et / 60, et % 60))

  }
  //////////////////////////////////////////////////////////////////////////////
} // End of Class definition
