package org.apache.hadoop.util;
import java.io.*;
import java.nio.file.*;
/** Local Windows test adapter: check actual IO instead of token-based ACL preflight. */
public class BasicDiskValidator implements DiskValidator {
 public static final String NAME="basic";
 public void checkStatus(File dir) throws DiskChecker.DiskErrorException {
  try { Files.createDirectories(dir.toPath()); try(DirectoryStream<Path> d=Files.newDirectoryStream(dir.toPath())){d.iterator().hasNext();} Path p=Files.createTempFile(dir.toPath(),"io-probe-",".tmp"); Files.write(p,new byte[]{1}); Files.readAllBytes(p); Files.delete(p); }
  catch(IOException e){throw new DiskChecker.DiskErrorException("Actual directory IO failed: "+dir,e);}
 }
}
