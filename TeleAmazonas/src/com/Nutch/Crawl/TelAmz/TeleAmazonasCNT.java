/**
 * 
 */
package com.Nutch.Crawl.TelAmz;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author surendra
 *
 */
public class TeleAmazonasCNT {

	/**
	 * 
	 */
	public TeleAmazonasCNT() {
		// TODO Auto-generated constructor stub
	}
	
	
	HTable ht=null;
	Scan sc=null;
	ResultScanner resc;
	String rownames=null,family=null,qualifier=null;
	
	
	public void TelAmzCNT()
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
					
					
				
					if(rownames.contains("/entretenimiento/") &&! rownames.contains("/oie/")  && !rownames.contains("/supernick/") && !rownames.endsWith("/entretenimiento/"))
					{
					
					if(family.equals("f")&& qualifier.equals("cnt"))
						
						{
						//System.out.println(rownames);	
						
						 
						
						new TeleAmzTVshow().TeleAShowCNT(rownames);
						
						 new TeleAmazonasRMTVSH().TeleARMTVSHCNT(rownames);
						
						new TeleAmzOtherLinks().TeleAVDTVSHCNT(rownames);
						}
						
					}
					
					
					
					else if(rownames.contains("/series/")&& !rownames.endsWith("/series/"))
					{
					
					if(family.equals("f")&& qualifier.equals("cnt"))
						
						{
						//System.out.println(rownames);	
						
						
						
						new TeleAmzTVshow().TeleAShowCNT(rownames);
						new TeleAmazonasRMTVSH().TeleARMTVSHCNT(rownames);
						new TeleAmzOtherLinks().TeleAVDTVSHCNT(rownames);
						}
						
					}
					 
					 
					////////////////////////////////////
					
					else if(rownames.contains("/telenovelas/")&& !rownames.endsWith("/telenovelas/"))
					{
					
					if(family.equals("f")&& qualifier.equals("cnt"))
						
						{
						//System.out.println(rownames);	
						
						
						
						new TeleAmzTVshow().TeleAShowCNT(rownames);
						 new TeleAmazonasTvShowCrew().TeleATvShowCrewCNT(rownames);
						 new TeleAmazonasTvShowPRGCrew().TeleATvShowProgCrewCNT(rownames);
						
						new TeleAmazonasRMTVSH().TeleARMTVSHCNT(rownames);
						new TeleAmzOtherLinks().TeleAVDTVSHCNT(rownames);
						
						}
					}
					////////////////////////////////////////
					
					
					
					else if(rownames.contains("/supernick/")&& !rownames.endsWith("/supernick/") && !rownames.contains("/entretenimiento/"))
					{
					
					if(family.equals("f")&& qualifier.equals("cnt"))
						
						{
						//System.out.println(rownames);	
						
						
						
						new TeleAmzTVshow().TeleAShowCNT(rownames);
						new TeleAmazonasRMTVSH().TeleARMTVSHCNT(rownames);
						
						new TeleAmzOtherLinks().TeleAVDTVSHCNT(rownames);
						}
					}
					
					
					/////////////////////////////////////////////////
					
					
					else if(rownames.contains("/oie/")&& !rownames.endsWith("/oie/") && !rownames.contains("/entretenimiento/"))
					{
					
					if(family.equals("f")&& qualifier.equals("cnt"))
						
						{
					//System.out.println(rownames);	
						
						
						
						new TeleAmzTVshow().TeleAShowCNT(rownames);
					new TeleAmazonasRMTVSH().TeleARMTVSHCNT(rownames);
					new TeleAmzOtherLinks().TeleAVDTVSHCNT(rownames);
						}
					}
					
					
					 if(rownames.contains("/peliculas/")&& !rownames.endsWith("/peliculas/") && !rownames.contains("/entretenimiento/"))
					{
					
					if(family.equals("f")&& qualifier.equals("cnt"))
						
						{
						//System.out.println(rownames);	
						
						
						
					new TeleAmazonasMovCNT().TeleAMovCNT(rownames);
					new TeleAmazonasCrew().TeleACrewCNT(rownames);
						new TeleAmazonasProgCrew().TeleAProgCrewCNT(rownames);
						
						new TeleAmzRichMedia().TeleARMCNT(rownames);
						}
					}
					
					
					
					
				}
			}
						
						
			
							

			
		}
		
		catch(Exception e)
		{
		
		
		
			e.printStackTrace();
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
	
}
