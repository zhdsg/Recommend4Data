package com.eastday.format

import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.mapred.{FileOutputFormat, InvalidJobConfException, JobConf}
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.mapreduce.security.TokenCache

/**
 * Created by admin on 2018/4/27.
 */
class RddOutputFormat extends MultipleTextOutputFormat[String,String]{

    override  def checkOutputSpecs(ignored:FileSystem,job :JobConf)={
      var  outDir:Path =FileOutputFormat.getOutputPath(job);
      if ((outDir == null) && (job.getNumReduceTasks() != 0)) {
        throw new InvalidJobConfException("Output directory not set in JobConf.");
      }
      if (outDir != null)
      {
        val  fs:FileSystem = outDir.getFileSystem(job);

        outDir = fs.makeQualified(outDir);
        FileOutputFormat.setOutputPath(job, outDir);

        val path :Array[Path]=Array(outDir)
        TokenCache.obtainTokensForNamenodes(job.getCredentials(), path, job);
//        if (fs.exists(outDir)) {
//          throw new FileAlreadyExistsException("Output directory " + outDir + " already exists");
//        }
      }


    }
}
