package edu.lab;
import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import org.apache.hadoop.fs.*;
/** Windows local-only compatibility adapter; OS file IO still enforces access. */
public class WindowsLocalFileSystem extends LocalFileSystem {
 public WindowsLocalFileSystem(){super(new NioRaw());}
 public static class NioRaw extends RawLocalFileSystem {
  public FileStatus[] listStatus(org.apache.hadoop.fs.Path p)throws IOException {
   File f=pathToFile(p); if(!f.isDirectory()) return new FileStatus[]{getFileStatus(p)};
   List<FileStatus> result=new ArrayList<>();
   try(DirectoryStream<java.nio.file.Path> ds=Files.newDirectoryStream(f.toPath())){for(java.nio.file.Path x:ds)result.add(getFileStatus(new org.apache.hadoop.fs.Path(x.toUri())));}
   return result.toArray(new FileStatus[0]);
  }
  public boolean delete(org.apache.hadoop.fs.Path p, boolean recursive)throws IOException {
   java.nio.file.Path x=pathToFile(p).toPath(); if(!Files.exists(x,LinkOption.NOFOLLOW_LINKS))return false;
   if(!recursive || !Files.isDirectory(x,LinkOption.NOFOLLOW_LINKS)){Files.delete(x);return true;}
   Files.walkFileTree(x,new SimpleFileVisitor<java.nio.file.Path>(){
    public FileVisitResult visitFile(java.nio.file.Path f,BasicFileAttributes a)throws IOException {Files.delete(f);return FileVisitResult.CONTINUE;}
    public FileVisitResult postVisitDirectory(java.nio.file.Path d,IOException e)throws IOException{if(e!=null)throw e;Files.delete(d);return FileVisitResult.CONTINUE;}
   }); return true;
  }
 }
}
