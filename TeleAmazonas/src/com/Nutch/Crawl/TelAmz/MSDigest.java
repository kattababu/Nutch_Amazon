package com.Nutch.Crawl.TelAmz;

import java.security.MessageDigest;

/**
 * @author surendra
 *
 */
public class MSDigest {

	/**
	 * 
	 */
	public MSDigest() {
		// TODO Auto-generated constructor stub
	}
	String md5s=null;
	

	
    public void MD5(String names)throws Exception
    {
   // String password = "123456";

        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(names.getBytes());

        byte byteData[] = md.digest();

        	        //convert the byte to hex format method 1
        StringBuffer hexString = new StringBuffer();
    	for (int i=0;i<byteData.length;i++) {
    		String hex=Integer.toHexString(0xff & byteData[i]);
   	     	if(hex.length()==1) hexString.append('0');
   	     	hexString.append(hex);
    	}
    	md5s=hexString.toString();
    
}


}