package com.Nutch.Crawl.TelAmz;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.List;
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

public class TeleAmazonasWMCNT {

	public TeleAmazonasWMCNT() {
		// TODO Auto-generated constructor stub
	}
	HTable ht=null;
	Scan sc=null;
	ResultScanner resc;
	String rownames=null,family=null,qualifier=null,content=null,Splitter_SK=null,Description=null,Splitter_Text=null;
	String Splitter_TextI2=null,Splitter_TextI1=null,Splitter_TextI2a=null;
	
	MSDigest msd=new MSDigest();
	static FileOutputStream fos=null;
	static PrintStream ps=null;
	static File file=null;
	/*
	static 
	{
				
				FileStore.MovieTable("movie");
		 

	}
	
*/
	
	/*
	
	static 
	{
				
				FileStore.MovieTable("movie");
		 

	}
	
	*/
	
	public void TeleAWMCNT(String names)
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
						//div.wpb_text_column
						
						//Elements els=document.select("div.wpb_wrapper");
						Elements els=Xsoup.compile("//div[@class='wpb_wrapper']").evaluate(document).getElements();
						
						
						for(Element el:els)
						{
							
							//Element eltitle=el.select("h3").first();
							String title=Xsoup.compile("/h3/strong/text()|/h3/text()").evaluate(el).get();
							if(title!=null)
							{
							
							
							String rtitle=title.replace(",", "").trim();
							
							//System.out.println(rtitle);
							////////////////Movie_Sk/////////////////
							msd.MD5(rtitle.trim());
							
							System.out.print(msd.md5s+"#<>#");
							
							/////////////////////Movie_title/////////////////////
							
							
							System.out.print(rtitle.trim());
							
							System.out.print("#<>#");
														
							////////////////Movie_Original_title/////////////////
							
							System.out.print("#<>#");
							
							
							////////////////Movie_other_Titles/////////////////
							
							
							//Element elotitle=el.select("h6").first();
							//String othertitle=elotitle.text();
							String othertitle=Xsoup.compile("/h6/strong/text()|/h6/text()").evaluate(el).get();
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
							
							
							List<String> Descriptions=Xsoup.compile("/p/strong/text()|/p/text()").evaluate(el).list();
							for(String Descript:Descriptions)
							{
								Description=Descript.replace("Sinopsis:","").replaceFirst("^ ", "").trim();
								System.out.print(Description.trim());
							}
							System.out.print("#<>#");
							
							
							////////////////// Movie_ Genres//////////////////////
							
							
							String genres=Xsoup.compile("/h5/strong/text()|/h5/text()").evaluate(el).get();
							SplitText(genres);
							System.out.print(Splitter_Text.replace("-", "<>").replaceAll(" ", "").trim());
							
							//System.out.println(genres);
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
							String Prod_count=eloPCountry.text();
							
							
							SplitCountry(Prod_count);
							
							System.out.print(Splitter_Text.trim());
							System.out.print("#<>#");
							
							
							///////////////// Aux_Info///////////////////////////
							
							Element eloDate=el.select("h4 > em").first();
							String date=eloDate.text();
							
							SplitAuxDate(date);
							
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
						
						
						/*
						for(Element el:els)
						{
							Element eltitle=el.select("h3").first();
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
									System.out.print(Splitter_Text.trim());
									
									
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
						
						*/
						
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
		if(names!=null)
		{
		if(names.contains(" y "))
		{
			String[] splits=names.split(" y ");
			String Splitter_Text1=splits[splits.length-1];
			String Splitter_Text2=splits[splits.length-2];
			SplitInternal(Splitter_Text1);
			SplitInternal2(Splitter_Text2);
			if(Splitter_Text1.contains(", ")||Splitter_Text2.contains(", "))
			{
				
				Splitter_Text=Splitter_TextI1+"<>"+Splitter_TextI2;
				//System.out.println(Splitter_Text);
			}
			
			
			
			
			/*
			
			String[] splits1=Splitter_Text1.split("\\, ");
			String Splitter_TextSplit1=splits1[splits1.length-2];
			
			
			String[] splits2=Splitter_Text2.split("\\, ");
			String Splitter_TextSplit2=splits2[splits2.length-2];
			
			Splitter_Text=Splitter_TextSplit1+"<>"+Splitter_TextSplit2;
			System.out.println(Splitter_Text);
			*/
			
			//System.out.println("\n\n");
			
			
			
			
		}
		else
		{
			String[] splits=names.split("\\, ");
			Splitter_Text=splits[splits.length-2];
			
		}
			
		}
		
	}
	
	
	
	
	
	
	
	/////////////////////////////////////////////////////
	
	
	
	
public void SplitAuxDate(String names)
	
	{
		if(names!=null)
		{
		if(names.contains(" y "))
		{
			String[] splits=names.split(" y ");
			String Splitter_Text1=splits[splits.length-1];
			String Splitter_Text2=splits[splits.length-2];
			SplitInternal3(Splitter_Text1);
			SplitInternal4(Splitter_Text2);
			if(Splitter_Text1.contains(", ")||Splitter_Text2.contains(", "))
			{
				/*
				
				String regex="(.*)((\\d+)(:)(\\d+))(.*)";
				if(Splitter_TextI2.matches(regex))
				{
					
					if(Splitter_TextI2a==null)
					{
						Splitter_TextI2a="";
						
					}
					else
					{
					Splitter_TextI2a=Splitter_TextI2;
					}
					
					
					//System.out.println(Splitter_TextI2a);
					
				}

				*/
				
				
				Splitter_Text=Splitter_TextI1+" "+Splitter_TextI2;

				//System.out.println(Splitter_Text);
			}
			
			
			
			
			/*
			
			String[] splits1=Splitter_Text1.split("\\, ");
			String Splitter_TextSplit1=splits1[splits1.length-2];
			
			
			String[] splits2=Splitter_Text2.split("\\, ");
			String Splitter_TextSplit2=splits2[splits2.length-2];
			
			Splitter_Text=Splitter_TextSplit1+"<>"+Splitter_TextSplit2;
			System.out.println(Splitter_Text);
			*/
			
			//System.out.println("\n\n");
			
			
			
			
		}
		else
		{
			String[] splits=names.split("\\, ");
			Splitter_Text=splits[splits.length-1];
			
		}
			
		}
		
	}

	
	
	
	
	
	
	/////////////////////////////////////////////////////////

	
	
	
	public void SplitInternal(String names)
	{
		
		//String Splitter_TextI1=null;
		if(names.contains(", "))
		{
		String[] splits=names.split("\\, ");
		Splitter_TextI1=splits[splits.length-2];
		//System.out.println(Splitter_TextI1);
		}
		else
		{
			Splitter_TextI1=names;
			//System.out.println(Splitter_TextI1);
			
			
		}
		
	}
	
	public void SplitInternal2(String names)
	{
		//String Splitter_TextI2=null;
		if(names.contains(", "))
		{
		String[] splits=names.split("\\, ");
		Splitter_TextI2=splits[splits.length-2];
		//System.out.println(Splitter_TextI2);
		}
		else
		{
			Splitter_TextI2=names;
			//System.out.println(Splitter_TextI2);
			
			
		}
		
	}
	
	
	
	
	////////////////////////////////////////////////////////
	
	
	
	public void SplitInternal3(String names)
	{
		
		//String Splitter_TextI1=null;
		if(names.contains(", "))
		{
		String[] splits=names.split("\\, ");
		Splitter_TextI1=splits[splits.length-1];
		//System.out.println(Splitter_TextI1);
		}
		else
		{
			Splitter_TextI1="";
			
		}
		

		
		
		
	}
	
	public void SplitInternal4(String names)
	{
		//String Splitter_TextI2=null;
		if(names.contains(", "))
		{
		String[] splits=names.split("\\, ");
		Splitter_TextI2=splits[splits.length-1];
		//System.out.println(Splitter_TextI2);
		}
		else
		{
			Splitter_TextI2="";
			
		}
		
		
	}
}

