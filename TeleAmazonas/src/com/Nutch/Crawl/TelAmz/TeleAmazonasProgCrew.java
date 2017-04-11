package com.Nutch.Crawl.TelAmz;

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

//import us.codecraft.xsoup.Xsoup;

public class TeleAmazonasProgCrew {

	public TeleAmazonasProgCrew() {
		// TODO Auto-generated constructor stub
	}

	
	HTable ht=null;
	Scan sc=null;
	ResultScanner resc;
	String rownames=null,family=null,qualifier=null,content=null,Splitter_SK=null,Description=null,Splitter_Text=null;
	
	MSDigest msd=new MSDigest();
	
	
	
	public void TeleAProgCrewCNT(String names)
	{
		try
		{
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
						
						
						
						//String url=Xsoup.compile("//meta[@property='og:url']/@content").evaluate(document).get();
						
						
						Elements els=document.select("div.wpb_text_column");
						
						
						for(Element el:els)
						{
							Element eltitle=el.select("h3").first();
							String title=eltitle.text();
							msd.MD5(title.trim());
							String Program_sk=msd.md5s.trim();
							int i=1;
							
							
							Element elCrew=el.select("h5").last();
							String Crew=elCrew.text();
							SplitText(Crew);
							
							String[] crewtitles=Splitter_Text.split("\\, ");
							for(String crewtitle:crewtitles)
							{
								msd.MD5(crewtitle.trim());
								String Crew_sk=msd.md5s.trim();
								ProgCrewTab(Program_sk,Crew_sk,i);
								i=i+1;
								
								
							}
							
							
						}
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
					
				}
				catch(Exception e)
				{
					e.getMessage();
				}
			}
		
		}
	
	
	
	
	public void ProgCrewTab(String Prog_sk,String Crew_k,int incr)
	{
		
		//////////////////// Program_SK/////////////////////////
		System.out.print(Prog_sk.trim()+"#<>#");
		
////////////////////Program_Type/////////////////////////
		
		System.out.print("movie".trim()+"#<>#");
		
		
////////////////////Crew_SK/////////////////////////
		
		System.out.print(Crew_k.trim()+"#<>#");
		
		//////////////////Crew _Role//////////////
		
		System.out.print("actor".trim()+"#<>#");
		
		
		////////////////////// Crew_Description/////////////////
		
		System.out.print("#<>#");
		
		/////////////////////Crew_Role_Title////////////////
		
		System.out.print("#<>#");
		
		//////////////////////////Crew_Rank////////////////////
		
		System.out.print(incr+"#<>#");
		
		////////////////////////Crew_Aux_Info//////////////////
		
		System.out.print("#<>#");
		
		////////////////Crew_Created_At////////////////
		
		System.out.print("#<>#");
		
		////////////////////Crew_Modified_At////////////////
		
		System.out.print("#<>#");
		
		////////////////////Crew_Last_Seen/////////////
				
		System.out.print("#<>#");
		
		//////////////New Line////////////////
		
		System.out.print("\n");
		
		
		
		
		
		
		
		
	}
	
	public void SplitText(String names)
	{
		String[] splits=names.split("\\:");
		Splitter_Text=splits[splits.length-1];
		
	}
			
	
}
