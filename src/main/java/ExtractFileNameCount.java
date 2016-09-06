import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.SecurityUtil;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


/**
 * Created by rishikapoor on 26/08/2016.
 */
public class ExtractFileNameCount {

    public int run(String fileSystem, String folderName,String outputFile, String filterString) {

        Configuration conf = new Configuration();

        if("hdfs".equalsIgnoreCase(fileSystem)) {
            final String USER_KEY = "appuser.principal";
            final String KEYTAB_KEY = "appuser.keytab.filename";


            System.out.println(System.getProperty("user.dir"));

            // login with keytab
            final String principal = "bigdata-app-ecpdemolinkeddata-srvc@INTPROD.THOMSONREUTERS.COM";
            final String keytab = getClass().getResource("bigdata-app-ecpdemolinkeddata-srvc.keytab").toString();
            StringBuilder sb = new StringBuilder();
            InputStream inputStream =  getClass().getClassLoader().getResourceAsStream("bigdata-app-ecpdemolinkeddata-srvc.keytab");
            BufferedReader br = null;
            try {
                // read this file into InputStream

                 br = new BufferedReader(new InputStreamReader(inputStream));
                 sb = new StringBuilder();

                String line;
                while ((line = br.readLine()) != null) {
                    sb.append(line);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (inputStream != null) {
                    try {
                        inputStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

                //System.out.println("the keytab value is :" + keytab);


            conf.set(USER_KEY, principal);
            conf.set(KEYTAB_KEY,keytab);
            conf.set("mapreduce.map.output.compress", "true");
            conf.set("mapreduce.map.memory.mb", "4096");
            conf.set("mapreduce.reduce.memory.mb", "4096");
            conf.set("mapreduce.reduce.java.opts", "-Djava.net.preferIPv4Stack=true -Xmx3543348019");
            conf.set("mapreduce.map.java.opts", "-Djava.net.preferIPv4Stack=true -Xmx3543348019");
            // conf.set(HadoopIOConstants.IO_COMPRESSION_CODECS, BZip2Codec.class.getCanonicalName());
            conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()

            );
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName()

            );
            try

            {
                SecurityUtil.login(conf, KEYTAB_KEY, USER_KEY);
                System.out.println(" Trying to connect ..........");
            } catch (
                    IOException e
                    )

            {
                e.printStackTrace();
            }


            conf.addResource(getClass().getResource("core-site.xml")

            );
        }

        Path xmlInputPath = new Path(folderName);
        Path xmlOutPutPath = new Path(outputFile);

        FileSystem fs;
        try {
            if("hdfs".equalsIgnoreCase(fileSystem)) {
                fs = FileSystem.get(conf);
            }
            else{
                fs= FileSystem.getLocal(conf);

            }
            System.out.println("File System is " + fs.getUri());

            Job job = new org.apache.hadoop.mapreduce.Job(conf);

            RemoteIterator<LocatedFileStatus> ri = fs.listFiles(xmlInputPath, true);
            Map<String, Integer> counts = new HashMap<String, Integer>();
            ArrayList<String> count = new ArrayList<String>();
            while (ri.hasNext())

            {
                LocatedFileStatus fileStatus = ri.next();


                if (fileStatus.getPath().toString().contains(filterString)) {
                    System.out.println("chmod a+rwx on {}" + fileStatus.getPath().toString());
                    counts.put(fileStatus.getPath().toString(), 1);
                    count.add(fileStatus.getPath().toString());
                }


            }
            BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(xmlOutPutPath,true)));

            if(counts.size()==count.size())
            {

                br.write("There are no duplicates in the folder");
                br.write(String.valueOf(count.removeAll(counts.keySet())));
                br.flush();
//                System.out.println("There are no duplicates in the files");
//                System.out.println(count.removeAll(counts.keySet()));
            }
            else
            {
                br.write("There are duplicates in the folder");
                br.write(String.valueOf(count.removeAll(counts.keySet())));
                br.flush();
//                System.out.println("There are duplicates in the folder");
//                count.removeAll(counts.keySet());
            }

            br.close();


        }catch (IOException ex){
            System.out.println("Unable to connect to File System");
        }

        return 1;

    }



    public static void main(String[] args) throws Exception{

        ExtractFileNameCount extarctFiLeName = new ExtractFileNameCount();

        extarctFiLeName.run(args[0],args[1],args[2],args[3]);



    }


}
