/**
 * 
 */
package com.Nutch.Crawl.TelAmz;



import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.List;

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
import org.jsoup.select.Elements;
import org.jsoup.nodes.Element;

import us.codecraft.xsoup.Xsoup;

/**
 * @author surendra
 *
 */
public class TeleAmzRichMedia {

	/**
	 * 
	 */
	public TeleAmzRichMedia() {
		// TODO Auto-generated constructor stub
	}
	
	
	
	HTable ht=null;
	Scan sc=null;
	ResultScanner resc;
	String rownames=null,family=null,qualifier=null,content=null,Splitter_SK=null,Splitter_Text=null;
	
	MSDigest msd=new MSDigest();
	String rtitle=null,rimages=null,rdimens=null;
	
	static FileOutputStream fos=null;
	static PrintStream ps=null;
	static File file=null;
	int i=0;
	
	
	static 
	{
		FileStore.RichMediaTable("richmedia");
		
			}
	
	
	
	public void TeleARMCNT(String names)
	{
		try
		{
			
			
			fos = new FileOutputStream(FileStore.fileRM,true);
			ps = new PrintStream(fos);
			 System.setOut(ps);
			
			//System.out.println(names);
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
							//System.out.println(names);
						
						content=Bytes.toString(kv.getValue());
						Document document = Jsoup.parse(content);
						String url=Xsoup.compile("//meta[@property='og:url']/@content").evaluate(document).get();
						
						Elements el=Xsoup.compile("//div[@class='vc_row wpb_row vc_row-fluid']//div[@class='wpb_wrapper']").evaluate(document).getElements();
						
						for(Element xel:el)
						{
								
							String images=Xsoup.compile("//figure[@class='wpb_wrapper vc_figure']//img/@src").evaluate(xel).get();
							if(images!=null)
							{
								//System.out.println(images);	
								rimages=images;
								
								String  width=Xsoup.compile("//figure[@class='wpb_wrapper vc_figure']//img/@width").evaluate(xel).get();
								String  height=Xsoup.compile("//figure[@class='wpb_wrapper vc_figure']//img/@height").evaluate(xel).get();
								
								if(width!=null && height!=null)
								{
								String dimens=width+"x"+height;
								rdimens=dimens.trim();
								//System.out.println(rdimens);
								}
								
									
								Elements elt=xel.parents().select("h3").eq(i++);
								
								String titles=elt.text();
								if(titles!=null)
								{ rtitle=titles.replace(",", "").trim();
								//System.out.println(rtitle);
								//System.out.println(images);
								
								}
								
									RichMediaTab(rimages,rtitle,rdimens,url);
							
							}
							
						
					
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
			
	////////////////////////////////////////////////////////////////////
	
	
	
	
	
	public void RichMediaTab(String image_sk,String movie_sk,String dimens,String url) throws Exception
	
	{
		
		
		
		
		////////////////Rm_SK//////////////////////
		msd.MD5(image_sk.trim());
		
		System.out.print(msd.md5s.trim()+"#<>#");
		
		////////////////Program_SK//////////////////////
		
		msd.MD5(movie_sk.trim());
		System.out.print(msd.md5s.trim()+"#<>#");
		
		////////////////Program_Type//////////////////////
		
		System.out.print("movie".trim()+"#<>#");
		
		////////////////Media_Type//////////////////////
		
		System.out.print("image".trim()+"#<>#");
		
		////////////////Image_Type//////////////////////
		
		System.out.print("medium".trim()+"#<>#");
		
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
