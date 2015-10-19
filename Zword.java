import java.io.IOException;
import java.io.PrintStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.kohsuke.args4j.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.etinternational.hamr.sim.*;
import com.etinternational.hamr.*;
import com.etinternational.hamr.resource.*;
import com.etinternational.hamr.resource.file.*;
import com.etinternational.hamr.resource.hdfs.*;
import com.etinternational.hamr.store.Container.Type;
import com.etinternational.hamr.store.kv.TreeContainer;
import com.etinternational.hamr.store.kv.HashContainer;
//import com.etinternational.hamr.store.kv.list.KeyListStore;
import com.etinternational.hamr.store.kv.KeyValueStore;
import com.etinternational.hamr.store.kv.number.NumberStore;
import com.etinternational.hamr.transform.Transform;
import com.etinternational.hamr.join.SymmetricJoin;
import com.etinternational.hamr.store.Container;

/**
 * <p><pre>
 * Zword [option...] arguments...
 *  FILE/DIRECTORY/GLOB
 *  --help
 *  -c (--combiner)
 *  -n (--numOutputFiles=) NUMBER
 *  -o (--outputFile=) FILE
 *  -s (--sortKeys)
 *  -t (--fileSystemType=) [HDFS | POSIX]
 *  -z (--hadoopHome=) STRING
 *  </pre></p>
 */

public class Zword{
    private static final Logger LOG = LoggerFactory.getLogger(Zword.class);
    private static final String HADOOP_HOME = System.getenv("HADOOP_HOME");
    private static final String HDFS_SITE_CONFIG = "file://" + HADOOP_HOME + "/etc/hadoop/core-site.xml";
    private static final String KV_OUTPUT_FORMAT = "%1$s \t\t\t\t %2$s\n";
    private static enum FileSystemType{
	HDFS,
	    POSIX
	    };


    @Option(name="--help",usage="print this message",help=true)
	private boolean displayUsage = false;

    @Option(name="-t",usage="file system type,default=HDFS",aliases={"--fileSystemType="})
	private FileSystemType fileSystem = FileSystemType.HDFS;

    @Option(name="-z",usage="value of $HADOOP_HONE",metaVar="STRING")
	private String hadoopHome = HADOOP_HOME;

    @Option(name="-n",usage="number output files, default=1",metaVar="NUMBER")
	private int outputPartitions = 1;

    @Option(name="-o",usage="output file name (REQUIRED)",required=true,metaVar="FILE")
	private String outputFile = null;

    @Option(name="-s",usage="input spam",required=true,metaVar="STRING")
	private String inputFileS = null;

    @Option(name="-h",usage="input ham",required=true,metaVar="STRING")
	private String inputFileH = null;

    //@Argument(usage="input file,directory,glob (one or more)",required=true,metaVar="FILE/DIRECTORY/GLOB")
    //private final String[] inputFiles = null;

    //自定义Transform，两个输出口
    static final class TransH extends Flowlet{
	public TransH(){
	    super();
	    add(
		new PushSlot<Long, String>(){
		    Pattern p = Pattern.compile("[a-zA-Z]+");
		    @Override
			public void accept(Long key, String value, Flow flow) throws IOException {
			Matcher m = p.matcher(value.toLowerCase());
			while(m.find()){
			    if(m.group().length() > 20 || m.group().equals("a") || m.group().equals("the"))
				continue;
			    flow.push(pushHS, m.group(), "H");
			    flow.push(pushWAC, "H", 1L);
			}
		    }
		});

	}
	public PushPort<String, Long> pushWAC = add(new PushPort<String, Long>());
	public PushPort<String, String> pushHS = add(new PushPort<String, String>());
    }

    static final class TransS extends Flowlet{
	public TransS(){
	    super();
	    add(
		new PushSlot<Long, String>(){
		    Pattern p = Pattern.compile("[a-zA-Z]+");
		    @Override
			public void accept(Long key, String value, Flow flow) throws IOException{
			Matcher m = p.matcher(value.toLowerCase());
			while(m.find()){
			    if(m.group().length() > 20 || m.group().equals("a") || m.group().equals("the"))
				continue;
			    flow.push(pushHS, m.group(), "S");
			    flow.push(pushWAC, "S", 1L);
			}
		    }
		});
	}
	public PushPort<String, Long> pushWAC = add(new PushPort<String, Long>());
	public PushPort<String, String> pushHS = add(new PushPort<String, String>());
    }


    static final class TransT extends Flowlet{
	public TransT(){
	    super();
	    IterSlot = add(new PushSlot<String, Iterable<String>>(){
		    @Override
		    public void accept(String key, Iterable<String> values, Flow flow) throws IOException {

			Long countH = 0L;
			Long countS = 0L;
			for(String tmp : values){
			    if(tmp.equals("S"))
				countS ++;
			    else
				countH ++;
			}

			if(countS > 500 || countH > 500){
			    flow.push(pushHS, key, countH + "," + countS);
			    flow.push(pushWAC, "H", countH);
			    flow.push(pushWAC, "S", countS);
			}

		    }
		});
	}
	public final PushSlot<String, Iterable<String>> IterSlot;
	public final PushPort<String, Long> pushWAC = add(new PushPort<String, Long>());
	public final PushPort<String, String> pushHS = add(new PushPort<String, String>());
    }

    void run() throws Exception{
	org.apache.hadoop.conf.Configuration hdfsConfig = null;

	ResourceWriter<String, Long> writer = null;

	try{
	    HAMR.initialize(new SimulationConfig());
	    Workflow workflow = new Workflow();

	    hdfsConfig = new org.apache.hadoop.conf.Configuration();
	    hdfsConfig.addResource(new org.apache.hadoop.fs.Path(getHdfsSiteConfig()));
	    //	LOG.info("Configuration:{}", getHdfsSiteConfig());

	    ResourceReader<Long, String> readerH = new HdfsResourceReader<>(hdfsConfig, inputFileH);
	    ResourceReader<Long, String> readerS = new HdfsResourceReader<>(hdfsConfig, inputFileS);






	    final Transform<Long, String, String, String> oneWordH = new Transform<Long, String, String, String>(){
		Pattern p = Pattern.compile("[a-zA-Z]+");

		@Override
		public void apply(Long key, String value, Flow context) throws IOException{
		    Matcher m = p.matcher(value.toLowerCase());
		    while(m.find()){
			if(m.group().length() > 20 || m.group().equals("a") || m.group().equals("the"))
			    continue;
			context.push(m.group(), "H");
		    }
		}
	    };

	    final Transform<Long, String, String, String> oneWordS = new Transform<Long, String, String, String>(){
		Pattern p = Pattern.compile("[a-zA-Z]+");
		@Override
		public void apply(Long key, String value, Flow context) throws IOException{
		    Matcher m = p.matcher(value.toLowerCase());
		    while(m.find()){
			if(m.group().length() > 20 || m.group().equals("a") || m.group().equals("the"))
			    continue;
			context.push(m.group(), "S");
		    }
		}
	    };




	    //存放单词和H S列表
	    StoreList wordList1 = new StoreList();

	    //用来存放spam,ham单词总数
	    NumberStore<String, Long> wordCount = new NumberStore<>(new HashContainer<>(Type.PERSISTENT, String.class, Long.class));

	    TransT countHS2 = new TransT();

	    KeyValueStore<String, String> wordList2 = new KeyValueStore<>(String.class, String.class);


	    Transform<String, String, String, String> num2per = new Transform<String, String, String, String>(){
		@Override
		public void apply(String key, String value, Flow flow) throws IOException {

		    Long allH = flow.pull("H");
		    Long allS = flow.pull("S");
		    String[] lin = value.split(",");
		    //System.out.println(key + "\t" + lin[0] + "-" + allH + " , " + lin[1] + "-" + allS);
		    flow.push(key, Double.parseDouble(lin[0])/allH + " , " + Double.parseDouble(lin[1])/allS);

		}
	    };

	    //测试
	    Transform<String, Iterable<String>, String, String> tt = new Transform<String, Iterable<String>, String, String>(){
		@Override
		public void apply(String key, Iterable<String> values, Flow flow) throws IOException {
		    System.out.println(key);
		}
	    };

	    Transform<String, Long, String, String> twordCount = new Transform<String, Long, String, String>(){
		@Override
		public void apply(String key, Long value, Flow flow) throws IOException {
		    System.out.println(key + "====" + value);
		}
	    };


	    writer = new HdfsResourceWriter<>(hdfsConfig, outputFile, KV_OUTPUT_FORMAT);
	    workflow.add(readerH,readerS,oneWordH,oneWordS, wordList1, countHS2, wordList2, wordCount, tt, num2per, twordCount, writer);

	    readerH.bindPush(oneWordH).synchronous();
	    readerS.bindPush(oneWordS).synchronous();
	    oneWordH.bindPush(wordList1.putSorted());
	    oneWordS.bindPush(wordList1.putSorted());

    	    wordList1.pushSorted().bind(countHS2);
	    countHS2.pushWAC.bind(wordCount.sum());
	    countHS2.pushHS.bind(wordList2);

	    wordList2.bindPush(num2per);
	    num2per.bindPull(wordCount);
	    num2per.bindPush(writer);

	    //wordCount.bindPush(twordCount);

	    num2per.setPartitionCount(1);
	    wordCount.setPartitionCount(1);
	    writer.setPartitionCount(1);
	    workflow.execute();

	}finally{
	    if(HAMR.isInitialized())
		HAMR.shutdown();
	}
    }

    public static void main(String[] args) throws Exception{
	Zword example = new Zword();
	CmdLineParser parser = new CmdLineParser(example);
	parser.setUsageWidth(80);

	try{
	    parser.parseArgument(args);
	    if(example.displayUsage){
		example.printUsage(parser, System.out, null);
		System.exit(0);
	    }

	    if(example.fileSystem == FileSystemType.HDFS && example.hadoopHome == null){
		String error = "'HADOOP_HOME' not found in environment,use -z (--hadoopHome=)";
		example.printUsage(parser, System.err, error);
		System.exit(1);
	    }
	}catch (Exception e){
	    example.printUsage(parser, System.err, "ERROR: " + e.getMessage());
	    throw e;
	}

	example.run();
    }

    void printUsage(CmdLineParser parser, PrintStream stream, String message){
	if(message != null){
	    stream.println(message);
	}
	stream.println(this.getClass().getSimpleName() + " [options...] arguments...");
	parser.printUsage(stream);
	stream.println();
    }

    String getHdfsSiteConfig(){
	if(HADOOP_HOME == null || !HADOOP_HOME.equals(hadoopHome)){
	    return "file://" + hadoopHome + "/etc/hadoop/core-site.xml";
	}else{
	    return HDFS_SITE_CONFIG;
	}
    }
}
