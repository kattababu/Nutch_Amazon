/**
 * 
 */
package com.Nutch.Crawl.TelAmz;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

//import com.sun.syndication.feed.rss.Image;

import us.codecraft.xsoup.Xsoup;

/**
 * @author surendra
 *
 */
public class TeleAmazonasChanCNT {

	/**
	 * 
	 */
	public TeleAmazonasChanCNT() {
		// TODO Auto-generated constructor stub
	}
	
	
	HTable ht=null;
	Scan sc=null;
	ResultScanner resc;
	String rownames=null,family=null,qualifier=null,content=null;
	
	MSDigest msd=new MSDigest();
	static FileOutputStream fos=null;
	static PrintStream ps=null;
	static File file=null;
	static String c_sk=null;
	
	static 
	{
		FileStore.ChannelsTable("channel");
		FileStore.RichMediaTable("richmedia");

	}
	
	
	public void TeleAChanCNT(String names)
	{
		try
		{
			
			//fos = new FileOutputStream(FileStore.fileChanles,true);
			//ps = new PrintStream(fos);
			//System.setOut(ps);
			
			
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
						
						
						
						String url=Xsoup.compile("//meta[@property='og:url']/@content").evaluate(document).get();
						//System.out.println(url);
						
						
						String title=Xsoup.compile("//div[@class='logo']//img/@alt").evaluate(document).get();
						//System.out.println(title);
						
						msd.MD5(title);
						c_sk=msd.md5s.trim();
						
						
						String description=Xsoup.compile("//meta[@property='og:description']/@content").evaluate(document).get();
						//System.out.println(description);
						
						
						
						String image=Xsoup.compile("//div[@class='logo']//img[2]/@src").evaluate(document).get();
						//System.out.println(image);
						
						
					//	String Cntimezone="UTC -5 hrs";
						
						
						ChannelTab(c_sk,title,description,image,url);
						
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
	
	
	public void TeleAChanRMCNT(String names)
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
						
						
						
						String url=Xsoup.compile("//meta[@property='og:url']/@content").evaluate(document).get();
						//System.out.println(url);
						
						
						String title=Xsoup.compile("//div[@class='logo']//img/@alt").evaluate(document).get();
						//System.out.println(title);
						
						
						
						
						String image=Xsoup.compile("//div[@class='logo']//img[2]/@src").evaluate(document).get();
						
						
						
						String width=Xsoup.compile("//div[@class='logo']//img[2]/@width").evaluate(document).get();
						String height=Xsoup.compile("//div[@class='logo']//img[2]/@height").evaluate(document).get();
						String dims=width+"x"+height;
						
						RichMediaTab(image,title,dims,url);
						
						
						//ChannelTab(c_sk,title,description,image,url);
						
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
	
	
	
	
	
	
	
	
						
	
	public void ChannelTab(String c_sk,String title,String desc,String img,String url)
	{
		
		
		//////////////////////////// Channnel_ SK////////////////////
		
		System.out.print(c_sk.trim()+"#<>#");
		
		
//////////////////////////// Channnel_Title///////////////////
		
		System.out.print(title.trim()+"#<>#");
		
		
//////////////////////////// Channnel_Description////////////////////
		
		System.out.print(desc.trim()+"#<>#");
		
		
//////////////////////////// Channnel_ Genres////////////////////
		
		System.out.print("#<>#");
		
		
//////////////////////////// Channnel_ SubGenres////////////////////
		
		System.out.print("#<>#");
		
//////////////////////////// Channnel_ Image////////////////////
		
		System.out.print(img.trim()+"#<>#");
		
		
//////////////////////////// Channnel_ time_zone_offset////////////////////
		
		System.out.print("UTC -5:00".trim()+"#<>#");
		
//////////////////////////// Channnel_ Reference_Url////////////////////
		
		System.out.print(url.trim()+"#<>#");
		
//////////////////////////// Channnel_ Created_At////////////////////
		
		System.out.print("#<>#");
		
//////////////////////////// Channnel_Modified_At////////////////////
		
		System.out.print("#<>#");
		
//////////////////////////// Channnel_Last_Seen////////////////////
		
		System.out.print("#<>#");
		
		/////////////////////////New_Line//////////////////
		
		System.out.print("\n");
		
		
		
		
		
		
		
		
	}
	
	
	
	
	
	
	
public void RichMediaTab(String image_sk,String movie_sk,String dimens,String url) throws Exception
	
	{
		
		
		
		
		////////////////Rm_SK//////////////////////
		msd.MD5(image_sk.trim());
		
		System.out.print(msd.md5s.trim()+"#<>#");
		
		////////////////Program_SK//////////////////////
		
		msd.MD5(movie_sk.trim());
		System.out.print(msd.md5s.trim()+"#<>#");
		
		////////////////Program_Type//////////////////////
		
		System.out.print("channel".trim()+"#<>#");
		
		////////////////Media_Type//////////////////////
		
		System.out.print("image".trim()+"#<>#");
		
		////////////////Image_Type//////////////////////
		
		System.out.print("small".trim()+"#<>#");
		
		////////////////Rm_Size//////////////////////
		
		System.out.print("#<>#");
		
		////////////////Rm_Dimensions//////////////////////
		
		System.out.print(dimens.trim()+"#<>#");
		
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


}
