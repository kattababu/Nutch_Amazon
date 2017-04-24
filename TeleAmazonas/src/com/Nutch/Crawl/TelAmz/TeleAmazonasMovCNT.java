package com.Nutch.Crawl.TelAmz;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tika.language.LanguageIdentifier;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import us.codecraft.xsoup.Xsoup;

public class TeleAmazonasMovCNT {

	public TeleAmazonasMovCNT() {
		// TODO Auto-generated constructor stub
	}
	HTable ht=null;
	Scan sc=null;
	ResultScanner resc;
	String rownames=null,family=null,qualifier=null,content=null,Splitter_SK=null,Description=null,Splitter_Text=null;
	
	MSDigest msd=new MSDigest();
	static FileOutputStream fos=null;
	static PrintStream ps=null;
	static File file=null;
	
	
	
	static 
	{
				
				FileStore.MovieTable("movie");
		 

	}
	
	
	
	public void TeleAMovCNT(String names)
	{
		try
		{
			
			fos = new FileOutputStream(FileStore.fileM,true);
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
						//
						
						Elements els=document.select("div.wpb_text_column");
						
						
						
						for(Element el:els)
						{
							Element eltitle=el.select("h3").first();
							if(eltitle!=null)
							{
							
							String title=eltitle.text();
							
							
							
							String rtitle=title.replace(",", "").trim();
							
							////////////////Movie_Sk/////////////////
							msd.MD5(rtitle.trim());
							
							System.out.print(msd.md5s+"#<>#");
							
							/////////////////////Movie_title/////////////////////
							
							
							System.out.print(rtitle.trim());
							
							System.out.print("#<>#");
														
							////////////////Movie_Original_title/////////////////
							
							System.out.print("#<>#");
							
							
							////////////////Movie_other_Titles/////////////////
							Element elotitle=el.select("h6").first();
							String othertitle=elotitle.text();
							String dictothertitle=othertitle.replace("(", "").replace(")", "").trim();
							//System.out.println(dictothertitle);
							if(dictothertitle.isEmpty())
							{
								System.out.print("#<>#");
							}
							
							else
							{
							 System.out.print("{\"Other_title\""+":\""+dictothertitle.trim()+"\"}"+"#<>#");
							}
							
							//////////////////////// Movie Description//////////////////////
							
									Elements elsp=el.select("p").not("*[style*='text-align: center;']");
									{
										for(Element elp:elsp)
										{
											
											//String attr=elp.attributes().toString();
											//System.out.println(attr);
										 Description=elp.text().replace("Sinopsis:","").replaceFirst("^ ", "").trim();
										 System.out.print(Description.trim());
										 
										 //System.out.println("\n\n");
											
										}
										
										
							
							}
							
									

									System.out.print("#<>#");
									
									
									
									
							////////////////Movie_Genres/////////////////
									
									Element elGenres=el.select("h5").first();
									String genres=elGenres.text();
									SplitText(genres);
									System.out.print(Splitter_Text.replace("- ", "<>").trim());
									
									
									System.out.print("#<>#");
									
									
									
							////////////////Movie_Sub_Genres/////////////////
									
									System.out.print("#<>#");
									
									
									
							////////////////Movie_Category/////////////////
									
									System.out.print("#<>#");
									
									
							////////////////Movie_Duration/////////////////
									
									System.out.print("#<>#");
									
									
							////////////////Movie_Languages/////////////////
									
									System.out.print("#<>#");
									
									
							////////////////Movie_Original_languages/////////////////
									
									System.out.print("#<>#");
									
									
							////////////////Movie_Metadata_language/////////////////
									
									LanguageIdentifier identifier = new LanguageIdentifier(Description);
									String lang=identifier.getLanguage();
									Locale loc =new Locale(lang);
									String namevalue=loc.getISO3Language();
									if(identifier!=null)
									{
										
										//System.out.println("\n");
										////////////////////  More time we get 'spa'//////////////////
										//System.out.println(namevalue);
										
										//=> spa=> spanish
										namevalue="spanish";
									System.out.print(namevalue.substring(0, 1).toUpperCase()+namevalue.substring(1).trim());
									}
									
									
									System.out.print("#<>#");
									
									
							////////////////Movie_Aka/////////////////
									
									System.out.print("#<>#");
									
									
							////////////////Movie_Production_Country/////////////////
									
									
									Element eloPCountry=el.select("h4 > em").first();
									String country=eloPCountry.ownText();
									
									
									SplitCountry(country);
									System.out.print(Splitter_Text.replace(" y ", "<>").trim());
									
									
									System.out.print("#<>#");
									
									
							////////////////Movie_Aux_Info/////////////////
									Element eloDate=el.select("h4").first();
									String date=eloDate.text();
									
									
									SplitDate(date);
									
									 System.out.print("{\"Schedule_Date\""+":\""+Splitter_Text.trim()+"\"}");
										
									
									System.out.print("#<>#");
									
									
									
							////////////////Movie_Reference_Url/////////////////
									
									System.out.print(url.trim()+"#<>#");
									
									
							////////////////Movie_Created_At/////////////////
									
									System.out.print("#<>#");
									
									
							////////////////Movie_Modified_At/////////////////
									
									System.out.print("#<>#");
									
									
									
							////////////////Movie_Last_Seen////////////////
									
									System.out.print("#<>#");
									
									
							//////////////// new Line ///////////////////
									
									
									System.out.print("\n");

									
									
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
	
	
	public void SplitUrl(String names)
	{
		String[] splits=names.split("\\/");
		Splitter_SK=splits[splits.length-1];
		
	}
	
	public void SplitText(String names)
	{
		String[] splits=names.split("\\:");
		Splitter_Text=splits[splits.length-1];
		
	}

	public void SplitCountry(String names)
	{
		String[] splits=names.split("\\, ");
		Splitter_Text=splits[splits.length-2];
		
	}

	public void SplitDate(String names)
	{
		String[] splits=names.split("\\, ");
		Splitter_Text=splits[splits.length-1];
		
	}
}
