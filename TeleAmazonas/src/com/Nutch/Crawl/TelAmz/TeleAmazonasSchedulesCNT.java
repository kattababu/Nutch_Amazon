/**
 * 
 */
package com.Nutch.Crawl.TelAmz;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
//import java.util.TimeZone;
//import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
//import org.joda.time.DateTime;
//import org.joda.time.Days;
//import org.joda.time.Hours;
//import org.joda.time.Minutes;
//import org.joda.time.Seconds;
//import org.joda.time.format.DateTimeFormat;
//import org.joda.time.format.DateTimeFormatter;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import us.codecraft.xsoup.Xsoup;

/**
 * @author surendra
 *
 */


public class TeleAmazonasSchedulesCNT {

	/**
	 * 
	 */
	public TeleAmazonasSchedulesCNT() {
		// TODO Auto-generated constructor stub
	}
	
	HTable ht=null;
	Scan sc=null;
	ResultScanner resc;
	String rownames=null,family=null,qualifier=null,content=null;
	String results=null,resadd=null;
	
	MSDigest msd=new MSDigest();
	static FileOutputStream fos=null;
	static PrintStream ps=null;
	static File file=null;
	
	
	static 
	{
		FileStore.SchedulesTable("schedule");
	}
	
	
	public void TeleAScheduleCNT(String names)
	{
		try
		{
			
			fos = new FileOutputStream(FileStore.fileSched,true);
			ps = new PrintStream(fos);
			System.setOut(ps);
			
	
			Configuration config=HBaseConfiguration.create();
			ht=new HTable(config,"teleamz_webpage");
			sc=new Scan();
			resc=ht.getScanner(sc);
			for(Result res = resc.next(); (res != null); res=resc.next())
			{
				for(KeyValue kv:res.list())
				{
					
					rownames=Bytes.toString(kv.getRow());
					family=Bytes.toString(kv.getFamily());
					qualifier=Bytes.toString(kv.getQualifier());
					
					
					
					if(rownames.equals(names))	
					
					{
						if(family.equals("f")&& qualifier.equals("cnt"))
							
						{
						
						content=Bytes.toString(kv.getValue());
						Document document = Jsoup.parse(content);
						
						
						Elements els=Xsoup.compile("//div[@class='divParrilla']//table//tbody/tr").evaluate(document).getElements();
						
						for(Element el:els)
						{
							
							///////////// Channel_SK///////////////////////
							//System.out.println(TeleAmazonasChanCNT.c_sk);
							
							String Ch_sk=TeleAmazonasChanCNT.c_sk;
							
							///////////////////////// Program_Sk//////////////////////
							String title=Xsoup.compile("//a/@title").evaluate(el).get();
							
							String rtitle=title.replaceAll("[,(,)]","").trim();
							
							msd.MD5(rtitle.trim());
							
							//System.out.println(msd.md5s.trim());
							
							
							//////////////////////////Durations////////////////////////
							
							
							String durs=Xsoup.compile("//td/span/text()").evaluate(el).get();
							
								
							
							String durse=Xsoup.compile("//td[2]/span/text()").evaluate(el).get();
							
							///////////////////////////////////////////////////////
							
							
							SimpleDateFormat format = new SimpleDateFormat("hh:mm a");
							Date date1 = format.parse(durs);
							Date date2 = format.parse(durse);
							long Duration=0;
							
							long difference = date2.getTime() - date1.getTime(); 
							
							int days = (int) (difference / (1000*60*60*24));  
					        int hours = (int) ((difference - (1000*60*60*24*days)) / (1000*60*60)); 
					        long min = (int) (difference - (1000*60*60*24*days) - (1000*60*60*hours)) / (1000*60);
					        hours =  (hours < 0 ? -hours : hours);
					         if(durs.contains("PM") && durse.contains("AM") && min==0)
							{
							
					       Duration= (24-hours)*60+( min < 0 ? ((min-1)+60) : min);//+( sec < 0 ? -sec : sec);
					      }
					        else if(durs.contains("PM") && durse.contains("AM") && min!=0)
							{
					        	long mins=( min < 0 ? ((min-0)) : min);
					        	long hourss=(24-hours)*60;
							
					       Duration= hourss+mins;
					    	  
							}
					        
					        else

					        {
					        	 Duration= hours*60+( min < 0 ? ((min-1)+60) : min);//+( sec < 0 ? -sec : sec);
					        	
					        }
					        
					        
					      //  System.out.println(Duration);
					           ///////////////////////////////////////////////// StartDateTime////////////////////
							
					        String startdate=Xsoup.compile("/@id").evaluate(el).get();
					      // System.out.println(startdate);
					        
					        if(!startdate.contains("2017"))
					        {
					        	//SplitTime(startdate);
					        	
					        	String rest=startdate.substring(0, startdate.length()-8);
					        	String rest2=startdate.substring(rest.length()-1,startdate.length());
					        	//System.out.println(rest2);
					        	String resadd=rest+"04-2017"+rest2;
					        	//System.out.println(resadd);
					        	
					        	SplitTime(resadd);
					        	//System.out.println(results);
					        	
					        	
					        }
					        else
					        {
					        	SplitTime(startdate);
					        	//System.out.println(results);
					        }
							
					        
					        SchedulesTab(Ch_sk,msd.md5s,results,Duration);
							
							
							
							
													
						}
						
						
						
						
						
						
						}
					}
				}
			}
		}
		catch(Exception e)
		{
			//e.getMessage();
			e.printStackTrace();
		}
		
		finally
		{
			try
			{
				ht.close();
				resc.close();
				ps.close();
				fos.close();
				
			}
			catch(Exception e)
			{
				e.getMessage();
			}
		}
						
	
	}
	
	////////////////////////////////////////////////////////////////////// RichMedia FOR Schedule Case//////////////////////////////////
	
	
	public void TeleAScheduleRMCNT(String names)
	{
		try
		{
			
			fos = new FileOutputStream(FileStore.fileRM,true);
			ps = new PrintStream(fos);
			System.setOut(ps);
			
	
			Configuration config=HBaseConfiguration.create();
			ht=new HTable(config,"teleamz_webpage");
			sc=new Scan();
			resc=ht.getScanner(sc);
			for(Result res = resc.next(); (res != null); res=resc.next())
			{
				for(KeyValue kv:res.list())
				{
					
					rownames=Bytes.toString(kv.getRow());
					family=Bytes.toString(kv.getFamily());
					qualifier=Bytes.toString(kv.getQualifier());
					
					
					
					if(rownames.equals(names))	
					
					{
						if(family.equals("f")&& qualifier.equals("cnt"))
							
						{
						
						content=Bytes.toString(kv.getValue());
						Document document = Jsoup.parse(content);
						
						
						
						Elements els=Xsoup.compile("//div[@class='divParrilla']//table//tbody/tr").evaluate(document).getElements();
						
						for(Element el:els)
						{
							
							String Sc_Image=Xsoup.compile("//img/@src| //img  /@src").evaluate(el).get();
							String mImage=null;
							if(Sc_Image!=null)
							{
								mImage=Sc_Image;
								
								
								
								
								
								//System.out.println(mImage);
								///////////////////////// Program_Sk//////////////////////
								String title=Xsoup.compile("//a/@title").evaluate(el).get();
								
								String rtitle=title.replaceAll("[,(,)]","").trim();
								
								//System.out.println(rtitle);
								
								
								String url=Xsoup.compile("//meta[@property='og:url']/@content").evaluate(document).get();
								
								
								ScheduleRMTab(mImage,rtitle,url);
								
								
							}
							
							//msd.MD5(rtitle.trim());
							
							
							
							
							
							
							
							
							
							
							
							
							//System.out.println(Sc_Image);
							
							//System.out.println(msd.md5s.trim());
							
							/*
							//////////////////////////Durations////////////////////////
							
							
							String durs=Xsoup.compile("//td/span/text()").evaluate(el).get();
							
								
							
							String durse=Xsoup.compile("//td[2]/span/text()").evaluate(el).get();
							
							///////////////////////////////////////////////////////
							
							
							SimpleDateFormat format = new SimpleDateFormat("hh:mm a");
							Date date1 = format.parse(durs);
							Date date2 = format.parse(durse);
							long Duration=0;
							
							long difference = date2.getTime() - date1.getTime(); 
							
							int days = (int) (difference / (1000*60*60*24));  
					        int hours = (int) ((difference - (1000*60*60*24*days)) / (1000*60*60)); 
					        long min = (int) (difference - (1000*60*60*24*days) - (1000*60*60*hours)) / (1000*60);
					        hours =  (hours < 0 ? -hours : hours);
					         if(durs.contains("PM") && durse.contains("AM") && min==0)
							{
							
					       Duration= (24-hours)*60+( min < 0 ? ((min-1)+60) : min);//+( sec < 0 ? -sec : sec);
					      }
					        else if(durs.contains("PM") && durse.contains("AM") && min!=0)
							{
					        	long mins=( min < 0 ? ((min-0)) : min);
					        	long hourss=(24-hours)*60;
							
					       Duration= hourss+mins;
					    	  
							}
					        
					        else

					        {
					        	 Duration= hours*60+( min < 0 ? ((min-1)+60) : min);//+( sec < 0 ? -sec : sec);
					        	
					        }
					        
					        
					      //  System.out.println(Duration);
					           ///////////////////////////////////////////////// StartDateTime////////////////////
							
					        String startdate=Xsoup.compile("/@id").evaluate(el).get();
					      // System.out.println(startdate);
					        
					        if(!startdate.contains("2017"))
					        {
					        	//SplitTime(startdate);
					        	
					        	String rest=startdate.substring(0, startdate.length()-8);
					        	String rest2=startdate.substring(rest.length()-1,startdate.length());
					        	//System.out.println(rest2);
					        	String resadd=rest+"04-2017"+rest2;
					        	//System.out.println(resadd);
					        	
					        	SplitTime(resadd);
					        	//System.out.println(results);
					        	
					        	
					        }
					        else
					        {
					        	SplitTime(startdate);
					        	//System.out.println(results);
					        }
							
					        
					        SchedulesTab(Ch_sk,msd.md5s,results,Duration);
							
							*/
							
							
													
						}
						
						
						
						
						
						
						}
					}
				}
			}
		}
		catch(Exception e)
		{
			//e.getMessage();
			e.printStackTrace();
		}
		
		finally
		{
			try
			{
				ht.close();
				resc.close();
				ps.close();
				fos.close();
				
			}
			catch(Exception e)
			{
				e.getMessage();
			}
		}
						
	
	}
	
//////////////////////////// Tabs/////////////////////////////////
	
	
	public void SchedulesTab(String C_Sk,String title,String Start_Date,long duration)
	{
		////////////////////// Schedule_Channel_SK/////////////////////
		System.out.print(C_Sk.trim()+"#<>#");
		
//////////////////////Schedule_Program_SK/////////////////////
		System.out.print(title.trim()+"#<>#");
		
	
//////////////////////Schedule_Program_Type/////////////////////
		System.out.print("tvshow".trim()+"#<>#");
		
		
//////////////////////Schedule_Start_Datetime/////////////////////
		System.out.print(Start_Date.trim()+"#<>#");
		
		
//////////////////////Schedule_Durations/////////////////////
		 long durations=(duration*60);
		 int i=(int)durations;
		
		System.out.print(i+"#<>#");
		
//////////////////////Schedule_Attributes/////////////////////
		System.out.print("#<>#");
		
//////////////////////Schedule_Created_At/////////////////////
		System.out.print("#<>#");
		
		
//////////////////////Schedule_Modified_At/////////////////////
		System.out.print("#<>#");
		
//////////////////////Schedule_Last_Seen/////////////////////
		System.out.print("#<>#");
		
		
		/////////////////////////New_Line/////////////////
		
		System.out.print("\n");




		



	
	
	
	}
	
	
	
	public void ScheduleRMTab(String image_sk,String title,String url) throws Exception
	
	{
		
		
		
		
		////////////////Rm_SK//////////////////////
		msd.MD5(image_sk.trim());
		
		System.out.print(msd.md5s.trim()+"#<>#");
		
		////////////////Program_SK//////////////////////
		
		msd.MD5(title.trim());
		//SplitUrl(url);
		System.out.print(msd.md5s.trim()+"#<>#");
		
		////////////////Program_Type//////////////////////
		
		System.out.print("tvshow".trim()+"#<>#");
		
		////////////////Media_Type//////////////////////
		
		System.out.print("image".trim()+"#<>#");
		
		////////////////Image_Type//////////////////////
		
		System.out.print("small".trim()+"#<>#");
		
		////////////////Rm_Size//////////////////////
		
		System.out.print("#<>#");
		
		////////////////Rm_Dimensions//////////////////////
		
		System.out.print("#<>#");
		
		////////////////Rm_Description//////////////////////
		
		System.out.print("#<>#");
		
		////////////////Rm_Image_Url//////////////////////
		
		System.out.print(image_sk.trim()+"#<>#");
		
		////////////////Rm_Reference_Url//////////////////////
		
		System.out.print(url.trim()+"#<>#");
		
		////////////////Aux_Info/////////////////////
		
		System.out.print("#<>#");
		
		////////////////Created_At//////////////////////
		
		System.out.print("#<>#");
		
		////////////////Modified_At//////////////////////
		
		System.out.print("#<>#");
		
		////////////////Last_Seen//////////////////////
		
		System.out.print("#<>#");
		
		//////////////// New Line/////////////////////
		
		System.out.print("\n");
		
		
	}

	
	
	/*
	
	public void SplitTime(String names)
	{
		String[] durst=names.split("\\:");
		String result=durst[durst.length-1];
		int res=Integer.parseInt(result);
		//System.out.println(result);
		String result2=durst[durst.length-2];
		int res2=Integer.parseInt(result2);
		results=res2*60+res;
		//System.out.println(results);
	}
	
	
	
	public void SplitTime2(String names)
	{
		String[] durst=names.split("\\:");
		String result=durst[durst.length-1];
		int res=Integer.parseInt(result);
		//System.out.println(result);
		String result2=durst[durst.length-2];
		int res2=Integer.parseInt(result2);
		results2=res2*60+res;
		//System.out.println(results);
	}
	*/
	
	public void SplitTime(String names)throws Exception
	{
		
		Locale locs=new Locale("es","SPANISH");
		Date  formatter = new SimpleDateFormat("EEEE-dd-MM-yyyy-hh-mm-a",locs).parse(names);
		   SimpleDateFormat formatter2=new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	  results
	  =formatter2.format(formatter);
	  
	
	}
	
	

}
