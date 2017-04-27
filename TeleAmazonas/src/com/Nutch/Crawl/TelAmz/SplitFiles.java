/**
 * 
 */
package com.Nutch.Crawl.TelAmz;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
//import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.io.FileUtils;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
//import com.jcraft.jsch.SftpException;

/**
 * @author surendra
 *
 */
public class SplitFiles {

	/**
	 * 
	 */
	public SplitFiles() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
//	static String DestSouc=null;
	
	static String tabn=null;
	
	/*final static String hostname="176.9.181.61";
	final static String username="interns";
	final static String password="hdrn59!";
	final static String destination="/home/interns/sath_GenFramework/juicer/spiders/OUTPUT/processing/";
	
	*/
	
	final static String hostname="office.headrun.com";
	final static String username="hrb";
	final static String password="satishdhawan16!";
	final static String destination="/Users/hrb/MVP_PROD_NUTCH/";
	
	
	///// Prd Machine...
	
	/*
	final static String hostname="176.9.181.50";
	final static String username="root";
	final static String password="hdrn^nutch";
	final static String destination="/katta/SplitTeleAmazonas/";
	
	*/
	
	public static void FileSplitS() {
		// TODO Auto-generated method stub
		
		
		 try{  
			 
			 File dir = new File(FileStore.filePath+"/");
				String[] extensions = new String[] { "queries" };
				//System.out.println("Getting all .txt and .jsp files in " + dir.getCanonicalPath()
					//	+ " including those in subdirectories");
				List<File> files = (List<File>) FileUtils.listFiles(dir, extensions, true);
				for (File file : files) {
					//System.out.println("file: " + file.getCanonicalPath());
					
					String fnamespath=file.getCanonicalPath();
					String filename=file.getName();
					RealSplit(fnamespath,filename);
				
				}
				
			 /*
			  // Reading file and getting no. of files to be generated  
			  String inputfile = file.getName();//  Source File Name.  
			  System.out.println(inputfile);
			  double nol = 3.0; //  No. of lines to be split and saved in each output file.  
			  File Infile = new File(inputfile);  
			  Scanner scanner = new Scanner(Infile);  
			  int count = 0;  
			  while (scanner.hasNextLine())   
			  {  
			   scanner.nextLine();  
			   count++;  
			  }  
			  System.out.println("Lines in the file: " + count);     // Displays no. of lines in the input file.  

			  double temp = (count/nol);  
			  int temp1=(int)temp;  
			  int nof=0;  
			  if(temp1==temp)  
			  {  
			   nof=temp1;  
			  }  
			  else  
			  {  
			   nof=temp1+1;  
			  }  
			  System.out.println("No. of files to be generated :"+nof); 
				// Displays no. of files to be generated.  

			  //-----------------------------------------------------------------------

	
		 
		 // Actual splitting of file into smaller files  

		  FileInputStream fstream = new FileInputStream(inputfile); DataInputStream in = new DataInputStream(fstream);  

		  BufferedReader br = new BufferedReader(new InputStreamReader(in)); String strLine;  

		  for (int j=1;j<=nof;j++)  
		  {  
		   FileWriter fstream1 = new FileWriter("/katta/CanalIN/"+"_"+j+".txt");     // Destination File Location  
		   BufferedWriter out = new BufferedWriter(fstream1);   
		   for (int i=1;i<=nol;i++)  
		   {  
		    strLine = br.readLine();   
		    if (strLine!= null)  
		    {  
		     out.write(strLine);   
		     if(i!=nol)  
		     {  
		      out.newLine();  
		     }  
		    }  
		   }  
		   out.close();  
		  }  

		  in.close();  
		  */
		 
		 }
				
		 catch (Exception e)  
		 {  
		  System.err.println("Error: " + e.getMessage());  
		 }  
		 
		 

		}  
		
	
	
	public static  void RealSplit(String name,String fname)
	{
		try
		{
			// Reading file and getting no. of files to be generated  
			  String inputfile = name;//  Source File Name.  
			  //System.out.println(inputfile);
			  double nol = 1000.0; //  No. of lines to be split and saved in each output file.  
			  File Infile = new File(inputfile);  
			  Scanner scanner = new Scanner(Infile);  
			  int count = 0;  
			  while (scanner.hasNextLine())   
			  {  
			   scanner.nextLine();  
			   count++;  
			  }  
			 // System.out.println("Lines in the file: " + count);     // Displays no. of lines in the input file.  

			  double temp = (count/nol);  
			  int temp1=(int)temp;  
			  int nof=0;  
			  if(temp1==temp)  
			  {  
			   nof=temp1;  
			  }  
			  else  
			  {  
			   nof=temp1+1;  
			  }  
			 // System.out.println("No. of files to be generated :"+nof); 
				// Displays no. of files to be generated.  

			  //-----------------------------------------------------------------------
			  
//			   Remote Connection Files://///////////////////
			  
			  JSch jsch = new JSch();
			  //  Session session = jsch.getSession("interns", "176.9.181.61",22);//.getSession("hrb", "10.152.232.1", 22); //port is usually 22
			   // session.setPassword("hdrn59!");
			  Session session = jsch.getSession(username,hostname,22);//.getSession("hrb", "10.152.232.1", 22); //port is usually 22
			   session.setPassword(password);
			   
			    java.util.Properties config = new java.util.Properties(); 
			    config.put("StrictHostKeyChecking", "no");
			    session.setConfig(config);
			    //session.
			  // Session.put("StrictHostKeyChecking", "no");
			    session.connect();
			    Channel channel = session.openChannel("sftp");
			    channel.connect();
			    ChannelSftp cFTP = (ChannelSftp) channel;
			    JSch.setConfig("StrictHostKeyChecking", "no");
			   // cFTP.cd("/home/interns/sath_GenFramework/juicer/spiders/OUTPUT/processing/");
			    cFTP.cd(destination);
			 

	
		 
		 // Actual splitting of file into smaller files  

		  FileInputStream fstream = new FileInputStream(inputfile); DataInputStream in = new DataInputStream(fstream);  

		  BufferedReader br = new BufferedReader(new InputStreamReader(in)); String strLine;  
		  
		 //String DestSource= 

		  for (int j=1;j<=nof;j++)  
		  {  
			  SplitFile(fname);
			  
			 // DestSouc="Canal10_Terminal_";
			  
			  
			  
			  
			  OutputStream strm = cFTP.put(FileStore.filename+tabn+"_"+GetCurrentTimeStamp().replace(":","").replace(".","")+".queries");
		  //FileWriter fstream1 = new FileWriter(DestSouc); 
		  //cFTP.// Destination File Location  
		   BufferedWriter out = new BufferedWriter(new PrintWriter(strm));   
		   for (int i=1;i<=nol;i++)  
		   {  
		    strLine = br.readLine();   
		    if (strLine!= null)  
		    {  
		     out.write(strLine);   
		     if(i!=nol)  
		     {  
		      out.newLine();  
		     }  
		    }  
		   }  
		   out.close();  
		  }  

		  in.close();  
		  cFTP.disconnect();
		    session.disconnect();
		  
		 
		 }
				
		 catch (Exception e)  
		 { 
			 
			 e.printStackTrace();
		  //System.err.println("Error: " + e.getMessage());  
		 }  
		 
		 /*
		 finally
		 {
			 try
			 {
			 
			 JSch jsch = new JSch();
			    Session session = jsch.getSession("hrb", "office.headrun.com");//.getSession("hrb", "10.152.232.1", 22); //port is usually 22
			    session.setPassword("satishdhawan16!");
			    java.util.Properties config = new java.util.Properties(); 
			    config.put("StrictHostKeyChecking", "no");
			    session.setConfig(config);
			    //session.
			  // Session.put("StrictHostKeyChecking", "no");
			    session.connect();
			    Channel channel = session.openChannel("sftp");
			    channel.connect();
			    ChannelSftp cFTP = (ChannelSftp) channel;
			    JSch.setConfig("StrictHostKeyChecking", "no");
			    String sourceFile = DestSouc, targetFile = "/Users/hrb/sathwick_nutch_files";
			    

			        cFTP.put(sourceFile , targetFile );
			        
			        cFTP.disconnect();
				    session.disconnect();
			    } catch (SftpException e) {
			        // TODO Auto-generated catch block
			        e.printStackTrace();
			    }
			    catch (Exception e) {
			        // TODO Auto-generated catch block
			        e.printStackTrace();
			    }

			    
			}
			*/

		 }
	
	
	
	
	public static void SplitFile(String name)
	{
		String[] f=name.split("\\_");
		tabn=f[f.length-2];
		//System.out.println(tabn);
	}
	
	
	public static String GetCurrentTimeStamp() {
        SimpleDateFormat sdfDate = new SimpleDateFormat(
                "yyyyMMdd'T'HH:mm:ss.SSSSS");// dd/MM/yyyy
        Date now = new Date();
        String strDate = sdfDate.format(now);
        return strDate;
    }

	}
	


