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
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import us.codecraft.xsoup.Xsoup;

/**
 * @author surendra
 *
 */
public class TeleAmzOtherLinks {

	/**
	 * 
	 */
	public TeleAmzOtherLinks() {
		// TODO Auto-generated constructor stub
	}
	
	
	HTable ht=null;
	Scan sc=null;
	ResultScanner resc;
	String rownames=null,family=null,qualifier=null,content=null,Splitter_SK=null,DimensM=null,image_type=null;
	
	MSDigest msd=new MSDigest();
	
	static FileOutputStream fos=null;
	static PrintStream ps=null;
	static File file=null;
	
	
	static
	{
		FileStore.OtherLinksTable("otherlinks");
	}
	
	
	public void TeleAVDTVSHCNT(String names)
	{
		try
		{
			
			
			
			fos = new FileOutputStream(FileStore.fileOL,true);
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
						
						
						Elements el=Xsoup.compile("//div[@class='wpb_wrapper']//iframe").evaluate(document).getElements();
						
						for(Element xel:el)
						{
	
							String videos=Xsoup.compile("/@src").evaluate(xel).get();
							
							//System.out.println(videos);
							
							
							OtherLTab(videos,url);
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
	
	
	public void OtherLTab(String video_url,String url_sk)throws Exception
	{
		
		
		//////////////// Other_Sk////////////////////
		msd.MD5(video_url.trim());
		
		System.out.print(msd.md5s.trim()+"#<>#");
		
		//////////////// Program_SK////////////////////
		SplitUrl(url_sk);
		
		
		System.out.print(Splitter_SK.trim()+"#<>#");
		
		////////////////Program_Type////////////////////
		
		System.out.print("tvshow".trim()+"#<>#");
		
		////////////////Url_Type////////////////////
		
		System.out.print("trailer".trim()+"#<>#");
		
		////////////////Other_Link_Url////////////////////
		
		System.out.print(video_url.trim()+"#<>#");
		
		////////////////Created_At////////////////////
		
		System.out.print("#<>#");
		
		////////////////Modified_At////////////////////
		
		System.out.print("#<>#");
		
		////////////////Last-Seen////////////////////
		
		System.out.print("#<>#");
		
		//////////////// New Line/////////////////
		
		System.out.print("\n");
		
		
		
		
		
		
		
		
	}

	
	public void SplitUrl(String names)
	{
		String[] splits=names.split("\\/");
		Splitter_SK=splits[splits.length-1];
		
	}
	
}
