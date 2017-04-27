/**
 * 
 */
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

/**
 * @author surendra
 *
 */
public class TeleAmzTVshow {

	/**
	 * 
	 */
	public TeleAmzTVshow() {
		// TODO Auto-generated constructor stub
	}
	
	

	HTable ht=null;
	Scan sc=null;
	ResultScanner resc;
	String sb="welcome",sb2=null,concat=null;
	
	String rownames=null,family=null,qualifier=null,content=null,Splitter_SK=null,Description=null,aux=null;
	
	static FileOutputStream fos=null;
	static PrintStream ps=null;
	static File file=null;
	int i=0;
	
	/*
	static
	{
		FileStore.TVShowTable("tvshow");
	}
	*/
	
	public void TeleAShowCNT(String names)
	{
		try
		{
			fos = new FileOutputStream(FileStore.fileTvshow,true);
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
						
						//Element elm=document.body();
						
						
						
						
		////////////////TVShow_Sk/////////////////
						
						String url=Xsoup.compile("//meta[@property='og:url']/@content").evaluate(document).get();
						if(url!=null)
						{
						//System.out.println(url);
						
						SplitUrl(url);
									
						System.out.print(Splitter_SK.trim()+"#<>#");
						
						
						//System.out.print("#<>#");
						
						
				////////////////TVShow_title/////////////////
						
						String titlename=Xsoup.compile("//div[@class='category-title']//span[@itemprop='name']/text()").evaluate(document).get();
						
						System.out.print(titlename.replace(",", "").trim()+"#<>#");
						
						
						//System.out.print("#<>#");
						
						
				////////////////TVShow_Original_title/////////////////
						
						System.out.print("#<>#");
						
						
				////////////////TVShow_other_Titles/////////////////
						
						System.out.print("#<>#");
						
						
				////////////////TVShow_Description/////////////////
						//Element els=elm.select("div.wpb_text_column wpb_content_element ").first();
						//System.out.println(els.data());
						
						//#ls-global > div.boxed-wrap.clearfix > div.boxed-content-wrapper.clearfix > div:nth-child(6) > div.main_container > div.main-col > div:nth-child(4) > div > div > div > div > div
						
						Elements els=document.select("div.wpb_text_column");
						
						//System.out.println(els.toString());
						String regex="(([a-zA-Z])*):(.*)((\\d+)(:)(\\d+))(.*)";
						
						for(Element el:els)
						{
							i=1;
							
									Elements elsp=el.select("p, h3").not("*[style*='text-align: center;']");
									{
										for(Element elp:elsp)
										{ 
											
											 Description=elp.text();
											 if(Description.contains("Reparto Principal:")||Description.contains("Elenco:"))
											 {
												 elp.remove();
											 }
											 else if(Description.matches(regex))
											 {
												 	 aux=elp.text();
													 StringConcat(aux,i++);
													  
												 elp.remove();
												 
												 
											 }
											 else
											 {
												
												 System.out.print(Description.trim());
											 }
										 
											
										 
										 //System.out.println("\n\n");
											
										}
										
										
							
							}
									
									
									
									
									
						}
						
						
						////////////////////////////////
						
						
						
						
						
						System.out.print("#<>#");
						
						
				////////////////TVShow_Genres/////////////////
						
						System.out.print("#<>#");
						
						
						
				////////////////TVShow_Sub_Genres/////////////////
						
						System.out.print("#<>#");
						
						
						
				////////////////TVShow_Category/////////////////
						
						System.out.print("#<>#");
						
						
				////////////////TVShow_Duration/////////////////
						
						System.out.print("#<>#");
						
						
				////////////////TVShow_Languages/////////////////
						
						System.out.print("#<>#");
						
						
				////////////////TVShow_Original_languages/////////////////
						
						System.out.print("#<>#");
						
						
				////////////////TVShow_Metadata_language/////////////////
						
						LanguageIdentifier identifier = new LanguageIdentifier(titlename);
						String lang=identifier.getLanguage();
						Locale loc =new Locale(lang);
						String namevalue=loc.getISO3Language();
						if(identifier!=null)
						{
							
							////////////////////  More time we get 'spa'//////////////////
							//System.out.println(namevalue);
							
							//=> spa=> spanish
							namevalue="spanish";
						System.out.print(namevalue.substring(0, 1).toUpperCase()+namevalue.substring(1).trim());
						}
						
						
						System.out.print("#<>#");
						
						
				////////////////TVShow_Aka/////////////////
						
						System.out.print("#<>#");
						
						
				////////////////TVShow_Production_Country/////////////////
						
						System.out.print("#<>#");
						
						
				////////////////TVShow_Aux_Info/////////////////
						
						if(aux!=null)
						{
						 System.out.print("{\"Schedule\""+":\""+concat.trim()+"\"}"+"#<>#");
						}
						else
						{
							
						
						System.out.print("#<>#");
						}
						
						
						
				////////////////TVShow_Reference_Url/////////////////
						
						System.out.print(url.trim()+"#<>#");
						
						
				////////////////TVShow_Created_At/////////////////
						
						System.out.print("#<>#");
						
						
				////////////////TVShow_Modified_At/////////////////
						
						System.out.print("#<>#");
						
						
						
				////////////////TVShow_Last_Seen////////////////
						
						System.out.print("#<>#");
						
						
				//////////////// new Line ///////////////////
						
						
						System.out.print("\n");
						
						
					
					
					
					/*
					
					String Descripts=Xsoup.compile("//div[@class='wpb_wrapper']/p/text()").evaluate(document).get();
					System.out.println(Descripts);
			*/
					
				
					
					
					
					//System.out.print("#<>#");
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
	
	public void  StringConcat(String names,int a)
	{
		if(names!=null)
		{
		
		if(a >= 1)
		{
			sb+=names+" ";
			concat=sb.replaceFirst("welcome", "");
			
			
			
			
				}
		/*
		else if(a > 1)
		{
			sb2=names;
			
		}*/
		
		}
		
		/*
		if(sb2!=null)
		{
			concat=sb+" "+sb2;
		//System.out.println(concat);
		}*/
		
		
		
	}
}
