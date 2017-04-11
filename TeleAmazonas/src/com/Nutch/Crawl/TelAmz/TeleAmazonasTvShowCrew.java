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

import us.codecraft.xsoup.Xsoup;

public class TeleAmazonasTvShowCrew {

	public TeleAmazonasTvShowCrew() {
		// TODO Auto-generated constructor stub
	}

	
	HTable ht=null;
	Scan sc=null;
	ResultScanner resc;
	String rownames=null,family=null,qualifier=null,content=null,Splitter_SK=null,Description=null,Splitter_Text=null;
	
	MSDigest msd=new MSDigest();
	
	
	
	public void TeleATvShowCrewCNT(String names)
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
						
						
						
						String url=Xsoup.compile("//meta[@property='og:url']/@content").evaluate(document).get();
						
						Elements els=document.select("div.wpb_text_column");
						
						for(Element el:els)
						{
							
									Elements elsp=el.select("p").not("*[style*='text-align: center;']");
									{
										for(Element elp:elsp)
										{
											
											 Description=elp.text();
											 if(Description.contains("Reparto Principal:"))
											 {
												 String splittext=elp.toString().replace("<br>", ",").replaceAll("<strong>", "").replaceAll("</strong>", "").replace("<p style='text-align: justify;'>", "").replace("</p>", "");
												 
												SplitText(splittext);
												String[] comasplit=Splitter_Text.split("\\,");
												for(String crewtitle:comasplit)
												{
													//System.out.println(crewnames);
													
													
													////////////////Crew_SK//////////////////////
													msd.MD5(crewtitle.trim());
													
													System.out.print(msd.md5s.trim()+"#<>#");								
													
													
													////////////////Crew_Name//////////////////////
													
													System.out.print(crewtitle.trim()+"#<>#");
													
													//////////////////Crew_Original_Name////////////////////
													
													System.out.print("#<>#");
													
													////////////////Crew_Description//////////////////////
													
													System.out.print("#<>#");
													
													////////////////Crew_Aka//////////////////////
													
													System.out.print("#<>#");
													
													//////////////////Crew_Gender////////////////////
													
													System.out.print("#<>#");
													
													////////////////Crew_Age//////////////////////
													
													System.out.print("#<>#");
													
													
													////////////////Crew_Blood_Group//////////////////////
													
													System.out.print("#<>#");
													
													//////////////////Crew_Birth_Date////////////////////
													
													System.out.print("#<>#");
													
													////////////////Crew_Birth_Place//////////////////////
													
													System.out.print("#<>#");
													
													
													////////////////Crew_Death_Date//////////////////////
													
													System.out.print("#<>#");
													
													//////////////////Crew_Death_Place////////////////////
													
													System.out.print("#<>#");
													
													////////////////Crew_Constellation//////////////////////
													
													System.out.print("#<>#");
													
													
													////////////////Crew_Country//////////////////////
													
													System.out.print("#<>#");
													
													//////////////////Crew_Occuption////////////////////
													
													System.out.print("#<>#");
													
													////////////////Crew_Biography//////////////////////
													
													System.out.print("#<>#");
													
													
													
													////////////////Crew_Height//////////////////////
													
													System.out.print("#<>#");
													
													//////////////////Crew_Weight////////////////////
													
													System.out.print("#<>#");
													
													////////////////Crew_Rating//////////////////////
													
													System.out.print("#<>#");
													
													//////////////Crew_Top_Related_Works//////////////////////
													
													System.out.print("#<>#");
													
													////////////////Crew _No_Ratings//////////////////////
													
													System.out.print("#<>#");
													
													////////////////Crew_Family_Members//////////////////////
													
													System.out.print("#<>#");
													
													////////////////Crew_Recent_Films//////////////////////
													
													System.out.print("#<>#");
													
													////////////////Crew_Images//////////////////////
													
													System.out.print("#<>#");
													
													////////////////Crew_Videos//////////////////////
													
													System.out.print("#<>#");
													
													////////////////Crew_Reference_Url//////////////////////
													
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
											
											
											
										}
										
							
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
		
	public void SplitText(String names)
	{
		String[] splits=names.split("\\:,");
		Splitter_Text=splits[splits.length-1];
		
	}

	
}
