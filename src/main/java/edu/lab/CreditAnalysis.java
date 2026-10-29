package edu.lab;

import java.io.*;
import java.net.URI;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

/** Five assignment tasks; all record processing is performed by Hadoop jobs. */
public class CreditAnalysis extends Configured implements Tool {
  static final String[] TRAIN={"CUST_ID","OPEN_ORG_NUM","bad_good","CUST_SAVING_AMT","CUST_FINA_AMT","CUST_PRIFINA_AMT","CUST_FOND_AMT","CUST_NADEBT_AMT","CUST_YBT_AMT","CUST_METAL_AMT","CUST_ASSET_AMT","CUST_DEBT_AMT","G_OS_PRCP_SUM","LOAN_FLAG","CRED_FLAG"};
  static final String[] TYPES={"存款","理财","私人理财","基金","国债","银保通","贵金属"};
  static final String[] LEVELS={"低资产","中等资产","较高资产","高资产"};
  static final BigDecimal ZERO=BigDecimal.ZERO;
  public enum Rows { VALID, BLANK, HEADER }

  // RFC-style escaped quotes and commas; input records must occupy one physical line.
  static String[] csv(String line) throws IOException {
    List<String> a=new ArrayList<>(); StringBuilder s=new StringBuilder(); boolean quote=false;
    for(int i=0;i<line.length();i++){char c=line.charAt(i); if(c=='"'){if(quote && i+1<line.length() && line.charAt(i+1)=='"'){s.append('"');i++;}else quote=!quote;}else if(c==','&&!quote){a.add(s.toString().trim());s.setLength(0);}else s.append(c);}
    if(quote)throw new IOException("CSV contains an unclosed quote / multiline record");a.add(s.toString().trim());return a.toArray(new String[0]);
  }
  static BigDecimal number(String s) throws IOException {
    if(s.isEmpty()||s.equalsIgnoreCase("nan")||s.equalsIgnoreCase("null")||s.equalsIgnoreCase("NA"))return ZERO;
    try{return new BigDecimal(s);}catch(NumberFormatException e){throw new IOException("Invalid amount: "+s,e);}
  }
  static class Row {
    String id,org,loan,cred; int label; BigDecimal[] money=new BigDecimal[10];
    Row(String[] a,boolean train)throws IOException{
      if(a.length!=(train?15:14))throw new IOException("Unexpected CSV field count: "+a.length);
      id=a[0];org=a[1].isEmpty()?"UNKNOWN":a[1]; if(id.isEmpty())throw new IOException("Empty customer ID");
      if(train){if(!a[2].equals("0")&&!a[2].equals("1"))throw new IOException("Invalid bad_good: "+a[2]);label=Integer.parseInt(a[2]);}
      int start=train?3:2;for(int j=0;j<10;j++)money[j]=number(a[start+j]);loan=a[start+10];cred=a[start+11];
    }
    BigDecimal asset(){return money[7];} BigDecimal debt(){return money[8];}
  }
  abstract static class CsvMapper<K,V> extends Mapper<LongWritable,Text,K,V>{
    protected boolean training=true;
    @Override public void map(LongWritable key,Text value,Context c)throws IOException,InterruptedException{
      String line=value.toString().replace("\uFEFF","");if(line.trim().isEmpty()){c.getCounter(Rows.BLANK).increment(1);return;}
      String[] a=csv(line);if(a[0].equals("CUST_ID")){c.getCounter(Rows.HEADER).increment(1);return;}
      Row row=new Row(a,training);process(row,c);c.getCounter(Rows.VALID).increment(1);
    }
    abstract void process(Row r,Context c)throws IOException,InterruptedException;
  }
  static BufferedReader cached(Configuration conf,String key)throws IOException{
    URI uri=URI.create(conf.get(key));File f=new File(uri.getFragment());
    if(f.isFile())return new BufferedReader(new InputStreamReader(new FileInputStream(f),StandardCharsets.UTF_8));
    Path p=new Path(uri.toString().split("#")[0]);return new BufferedReader(new InputStreamReader(p.getFileSystem(conf).open(p),StandardCharsets.UTF_8));
  }
  static double[] quartiles(Configuration c)throws IOException{
    try(BufferedReader b=cached(c,"credit.quartiles")){String[] a=b.readLine().split("\t");double[] q=new double[3];for(int i=0;i<3;i++)q[i]=Double.parseDouble(a[i].split(":")[1]);return q;}
  }
  static int level(double x,double[] q){return x<=q[0]?0:x<=q[1]?1:x<=q[2]?2:3;}
  public static class OrgMapper extends CsvMapper<Text,Text>{
    void process(Row r,Context c)throws IOException,InterruptedException{c.write(new Text(r.org),new Text("1,"+r.asset().toPlainString()+","+r.debt().toPlainString()+","+r.label));}
  }
  public static class OrgReducer extends Reducer<Text,Text,Text,Text>{
    public void reduce(Text k,Iterable<Text> vs,Context c)throws IOException,InterruptedException{
      long n=0,bad=0;BigDecimal a=ZERO,d=ZERO;for(Text t:vs){String[] v=t.toString().split(",");n+=Long.parseLong(v[0]);a=a.add(new BigDecimal(v[1]));d=d.add(new BigDecimal(v[2]));bad+=Long.parseLong(v[3]);}c.write(k,new Text(n+","+a.toPlainString()+","+d.toPlainString()+","+bad));
    }
  }
  public static class AverageMapper extends CsvMapper<Text,Text>{
    final Map<String,String[]> stats=new HashMap<>();
    protected void setup(Context c)throws IOException{try(BufferedReader b=cached(c.getConfiguration(),"credit.org")){String s;while((s=b.readLine())!=null){String[] v=s.split("\t");stats.put(v[0],v[1].split(","));}}}
    void process(Row r,Context c)throws IOException,InterruptedException{String[] s=stats.get(r.org);if(s==null)throw new IOException("Missing organization "+r.org);boolean high=r.asset().multiply(new BigDecimal(s[0])).compareTo(new BigDecimal(s[1]))>=0;c.write(new Text(r.org),new Text(high?"1,0":"0,1"));}
  }
  public static class AverageReducer extends Reducer<Text,Text,Text,Text>{
    public void reduce(Text k,Iterable<Text> vs,Context c)throws IOException,InterruptedException{long a=0,b=0;for(Text t:vs){String[] v=t.toString().split(",");a+=Long.parseLong(v[0]);b+=Long.parseLong(v[1]);}c.write(k,new Text(a+","+b));}
  }
  public static class QuantileMapper extends CsvMapper<DoubleWritable,LongWritable>{
    void process(Row r,Context c)throws IOException,InterruptedException{double a=r.asset().doubleValue();if(!Double.isFinite(a))throw new IOException("Asset outside double range");c.write(new DoubleWritable(a==0?0:a),new LongWritable(1));}
  }
  public static class QuantileReducer extends Reducer<DoubleWritable,LongWritable,NullWritable,Text>{
    long seen;long[] ranks=new long[3];double[] q=new double[3];
    protected void setup(Context c){long n=c.getConfiguration().getLong("credit.n",0);for(int i=0;i<3;i++)ranks[i]=(n*(i+1)+3)/4;}
    public void reduce(DoubleWritable k,Iterable<LongWritable> vs,Context c){long n=0;for(LongWritable v:vs)n+=v.get();for(int i=0;i<3;i++)if(seen<ranks[i]&&seen+n>=ranks[i])q[i]=k.get();seen+=n;}
    protected void cleanup(Context c)throws IOException,InterruptedException{if(seen!=c.getConfiguration().getLong("credit.n",0))throw new IOException("Sample count mismatch");c.write(NullWritable.get(),new Text("Q25:"+q[0]+"\tQ50:"+q[1]+"\tQ75:"+q[2]));}
  }
  public static class LayerMapper extends CsvMapper<IntWritable,Text>{
    double[] q;protected void setup(Context c)throws IOException{q=quartiles(c.getConfiguration());}
    void process(Row r,Context c)throws IOException,InterruptedException{StringJoiner s=new StringJoiner(",");for(int i=0;i<7;i++)s.add(r.money[i].toPlainString());c.write(new IntWritable(level(r.asset().doubleValue(),q)),new Text(s.toString()));}
  }
  public static class LayerReducer extends Reducer<IntWritable,Text,Text,Text>{
    final BigDecimal[][] sums=new BigDecimal[4][7];protected void setup(Context c){for(BigDecimal[] a:sums)Arrays.fill(a,ZERO);}
    public void reduce(IntWritable k,Iterable<Text> vs,Context c){for(Text t:vs){String[] a=t.toString().split(",");for(int i=0;i<7;i++)sums[k.get()][i]=sums[k.get()][i].add(new BigDecimal(a[i]));}}
    protected void cleanup(Context c)throws IOException,InterruptedException{
      for(int l=0;l<4;l++){final BigDecimal[] a=sums[l];BigDecimal total=ZERO;for(BigDecimal x:a)total=total.add(x);final BigDecimal[] ratios=new BigDecimal[7];for(int j=0;j<7;j++)ratios[j]=total.signum()==0?ZERO:a[j].divide(total,16,java.math.RoundingMode.HALF_UP);
        Integer[] order={0,1,2,3,4,5,6};Arrays.sort(order,(i,j)->{int cmp=ratios[j].compareTo(ratios[i]);return cmp!=0?cmp:Integer.compare(i,j);});for(int i:order)c.write(new Text(LEVELS[l]),new Text(TYPES[i]+"\t"+ratios[i].setScale(8,java.math.RoundingMode.HALF_UP).toPlainString()));
      }
    }
  }
  // Categorical Naive Bayes: signed log10 bins + org + flags + quartile layer.
  static String[] features(Row r,double[] q){String[] f=new String[14];for(int j=0;j<10;j++){double x=r.money[j].doubleValue();f[j]=(x<0?"-":"+")+(int)Math.floor(Math.log10(1+Math.abs(x)));}f[10]=r.org;f[11]=r.loan.isEmpty()?"UNKNOWN":r.loan;f[12]=r.cred.isEmpty()?"UNKNOWN":r.cred;f[13]=Integer.toString(level(r.asset().doubleValue(),q));return f;}
  public static class TrainMapper extends CsvMapper<Text,LongWritable>{
    double[] q;protected void setup(Context c)throws IOException{q=quartiles(c.getConfiguration());}
    void process(Row r,Context c)throws IOException,InterruptedException{c.write(new Text("C|"+r.label),new LongWritable(1));String[] f=features(r,q);for(int j=0;j<f.length;j++)c.write(new Text("F|"+r.label+"|"+j+"|"+f[j]),new LongWritable(1));}
  }
  public static class CountReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
    public void reduce(Text k,Iterable<LongWritable> vs,Context c)throws IOException,InterruptedException{long n=0;for(LongWritable v:vs)n+=v.get();c.write(k,new LongWritable(n));}
  }
  public static class PredictMapper extends CsvMapper<Text,IntWritable>{
    double[] q;long[] counts=new long[2];final Map<String,Long> model=new HashMap<>();final List<Set<String>> vocab=new ArrayList<>();
    protected void setup(Context c)throws IOException{training=false;q=quartiles(c.getConfiguration());for(int j=0;j<14;j++)vocab.add(new HashSet<>());try(BufferedReader b=cached(c.getConfiguration(),"credit.model")){String s;while((s=b.readLine())!=null){String[] a=s.split("\t");long n=Long.parseLong(a[1]);model.put(a[0],n);String[] k=a[0].split("\\|",4);if(k[0].equals("C"))counts[Integer.parseInt(k[1])]=n;else vocab.get(Integer.parseInt(k[2])).add(k[3]);}}}
    void process(Row r,Context c)throws IOException,InterruptedException{String[] f=features(r,q);double[] score=new double[2];for(int y=0;y<2;y++){score[y]=Math.log((counts[y]+1.0)/(counts[0]+counts[1]+2.0));for(int j=0;j<f.length;j++){long n=model.getOrDefault("F|"+y+"|"+j+"|"+f[j],0L);score[y]+=Math.log((n+1.0)/(counts[y]+vocab.get(j).size()+1.0));}}c.write(new Text(r.id),new IntWritable(score[1]>score[0]?1:0));}
  }
  void cache(Job j,Path p,String key,String alias)throws Exception{URI uri=new URI(p.getFileSystem(getConf()).makeQualified(p).toString()+"#"+alias);j.addCacheFile(uri);j.getConfiguration().set(key,uri.toString());}
  Job job(String name,Path input,Path output,Class<? extends Mapper> mapper,Class<? extends Reducer> reducer,Class<?> mk,Class<?> mv,Class<?> ok,Class<?> ov,int reducers)throws Exception{
    Job j=Job.getInstance(getConf(),name);j.setJarByClass(CreditAnalysis.class);j.setMapperClass(mapper);if(reducer!=null)j.setReducerClass(reducer);j.setMapOutputKeyClass(mk);j.setMapOutputValueClass(mv);j.setOutputKeyClass(ok);j.setOutputValueClass(ov);j.setNumReduceTasks(reducers);FileInputFormat.addInputPath(j,input);FileOutputFormat.setOutputPath(j,output);return j;
  }
  void execute(Job j)throws Exception{if(!j.waitForCompletion(true))throw new IOException("Job failed: "+j.getJobName());System.out.println("COMPLETED "+j.getJobName()+" "+j.getJobID()+" validRows="+j.getCounters().findCounter(Rows.VALID).getValue());}
  Path merge(Path dir,Path dest)throws IOException{FileSystem fs=dir.getFileSystem(getConf());try(FSDataOutputStream out=fs.create(dest,false)){for(FileStatus f:fs.listStatus(dir))if(f.getPath().getName().startsWith("part-"))try(FSDataInputStream in=fs.open(f.getPath())){org.apache.hadoop.io.IOUtils.copyBytes(in,out,65536,false);}}return dest;}
  public int run(String[] args)throws Exception{
    if(args.length!=3){System.err.println("Usage: hadoop jar credit-mapreduce.jar [generic -D options] <train.csv> <test.csv> <new-output-directory>");return 2;}
    Path train=new Path(args[0]),test=new Path(args[1]),out=new Path(args[2]);FileSystem fs=out.getFileSystem(getConf());if(fs.exists(out))throw new IOException("Output already exists: "+out);fs.mkdirs(out);
    int parallel=getConf().getInt("credit.reducers",2);
    Job t1=job("task1-org-statistics",train,new Path(out,"task1"),OrgMapper.class,OrgReducer.class,Text.class,Text.class,Text.class,Text.class,parallel);t1.setCombinerClass(OrgReducer.class);execute(t1);
    long n=t1.getCounters().findCounter(Rows.VALID).getValue();if(n==0)throw new IOException("Empty training set");getConf().setLong("credit.n",n);
    Path org=merge(new Path(out,"task1"),new Path(out,"_org.tsv"));
    Job t2=job("task2-above-below-average",train,new Path(out,"task2"),AverageMapper.class,AverageReducer.class,Text.class,Text.class,Text.class,Text.class,parallel);t2.setCombinerClass(AverageReducer.class);cache(t2,org,"credit.org","org.tsv");execute(t2);
    Job t3=job("task3-global-quantiles",train,new Path(out,"task3"),QuantileMapper.class,QuantileReducer.class,DoubleWritable.class,LongWritable.class,NullWritable.class,Text.class,1);execute(t3);
    Path q=merge(new Path(out,"task3"),new Path(out,"_quartiles.tsv"));
    Job t4=job("task4-asset-composition",train,new Path(out,"task4"),LayerMapper.class,LayerReducer.class,IntWritable.class,Text.class,Text.class,Text.class,1);cache(t4,q,"credit.quartiles","quartiles.tsv");execute(t4);
    Job model=job("task5-train-naive-bayes",train,new Path(out,"model"),TrainMapper.class,CountReducer.class,Text.class,LongWritable.class,Text.class,LongWritable.class,parallel);model.setCombinerClass(CountReducer.class);cache(model,q,"credit.quartiles","quartiles.tsv");execute(model);
    Path m=merge(new Path(out,"model"),new Path(out,"_model.tsv"));
    Job predict=job("task5-distributed-prediction",test,new Path(out,"task5"),PredictMapper.class,null,Text.class,IntWritable.class,Text.class,IntWritable.class,0);cache(predict,q,"credit.quartiles","quartiles.tsv");cache(predict,m,"credit.model","model.tsv");execute(predict);return 0;
  }
  public static void main(String[] args)throws Exception{System.exit(ToolRunner.run(new Configuration(),new CreditAnalysis(),args));}
}
