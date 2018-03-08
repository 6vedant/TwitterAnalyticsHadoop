import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
 
public class ListOfUnConsideredUsers {

	/**
	 * @return
	 * @throws IOException
	 */
	public static List<String> getUnConsideredUsersList() throws IOException { 
		List<String> unConsideredUsers = new ArrayList<String>();
		
		String uri = "hdfs://localhost://UnConsideredUsersList";
		
		Configuration configuration = new Configuration();
		System.out.println("Trying to get the file system object");
		
		URI uriObj = URI.create(uri);
		System.out.println("Got URI object "+uri);
		
		FileSystem fs = FileSystem.get(uriObj, configuration);
		FSDataInputStream fsDataInputStream = null;
		
		Path hdfsPath = new Path(uri);
		
		fsDataInputStream = fs.open(hdfsPath);
		//fsDataInputStream.seek(0);
		IOUtils.copyBytes(fsDataInputStream, System.out, 4096, false);
		System.out.println("****Byte Completion Completed****");

		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(hdfsPath)));
		
		try {
			  String lineOfFile;
			  lineOfFile=br.readLine();
			  
			  while (lineOfFile != null){
			    System.out.println("################ Line is###### "+lineOfFile);
			    unConsideredUsers.add(lineOfFile);
			    lineOfFile = br.readLine();
			  }
			} finally {
			  br.close();
			}
		
		return unConsideredUsers;
	}

}
