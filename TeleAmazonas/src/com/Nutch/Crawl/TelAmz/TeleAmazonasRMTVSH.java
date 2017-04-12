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
public class TeleAmazonasRMTVSH {

	/**
	 * 
	 */
	public TeleAmazonasRMTVSH() {
		// TODO Auto-generated constructor stub
	}
	
	static FileOutputStream fos=null;
	static PrintStream ps=null;
	static File file=null;
	
	
	HTable ht=null;
	Scan sc=null;
	ResultScanner resc;
	String rownames=null,family=null,qualifier=null,content=null,Splitter_SK=null,DimensM=null,image_type=null;
	
	MSDigest msd=new MSDigest();
	
	
	
	public void TeleARMTVSHCNT(String names)
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
						
						
						Elements el=Xsoup.compile("//div[@class='wpb_wrapper']//img").evaluate(document).getElements();
						
						for(Element xel:el)
						{
							String Images=Xsoup.compile("/@src").evaluate(xel).get();
							String width=Xsoup.compile("/@width").evaluate(xel).get();
							String height=Xsoup.compile("/@height").evaluate(xel).get();
							if(width!=null && height!=null)
							{
								String dimens=width+"x"+height;
								if(dimens.equals("100%x"))
								{
									String mfdim="1024x440".trim();
									DimensM=mfdim;
								}
								else
								{
									DimensM=dimens;
									
									
								}
								
							}
							
							int w=Integer.parseInt(width);
							
							
							if(w >= 1024)
							{
								image_type="poster".trim();
							}
							
							else if(w >= 770 && w < 1024)
							{
								image_type="large".trim();
							}
							
							else if(w >= 400 && w < 770)
							{
								image_type="medium".trim();
							}
							
							else if(w <= 399)
							{
								image_type="small".trim();
							}
							
							
							
							
							RichMediaTab(Images,DimensM,image_type,url);
							
							
							//System.out.println(Images);
							//System.out.println(DimensM);
							
							
						
						}
						/*
						List<String> Images=Xsoup.compile("//div[@class='wpb_wrapper']//img/@src").evaluate(document).list();
						{
							
							for(String Image:Images)
							{
							String width=Xsoup.compile("//div[@class='wpb_wrapper']//img/@width").evaluate(document).get();
							String height=Xsoup.compile("//div[@class='wpb_wrapper']//img/@height").evaluate(document).get();
							
							String dimens=width+"x"+height;
							
							System.out.println(Image);
							
							System.out.println(dimens);
							if(width!=null && height!=null)
							{
							if(dimens.equals("100%x"))
							{
								dimens="1024x440".trim();
								
								
							}
							
							//System.out.println(dimens);
							
							//RichMediaTab(Image,dimens,url);
							
							}
							}
							
							
						}
						
							
						
						*/
						
						
						
						
						}
							
						
												
					}
				}
			}
		}
		catch(Exception e)
		{
			e.getMessage();
			//e.printStackTrace();
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
			
	
			
	
	
	
	public void RichMediaTab(String image_sk,String dimens,String image_type,String url) throws Exception
	
	{
		
		
		
		
		////////////////Rm_SK//////////////////////
		msd.MD5(image_sk.trim());
		
		System.out.print(msd.md5s.trim()+"#<>#");
		
		////////////////Program_SK//////////////////////
		
		//msd.MD5(movie_sk.trim());
		SplitUrl(url);
		System.out.print(Splitter_SK.trim()+"#<>#");
		
		////////////////Program_Type//////////////////////
		
		System.out.print("tvshow".trim()+"#<>#");
		
		////////////////Media_Type//////////////////////
		
		System.out.print("image".trim()+"#<>#");
		
		////////////////Image_Type//////////////////////
		
		System.out.print(image_type.trim()+"#<>#");
		
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

	
	public void SplitUrl(String names)
	{
		String[] splits=names.split("\\/");
		Splitter_SK=splits[splits.length-1];
		
	}
	
}
