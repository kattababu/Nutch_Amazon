/**
 * 
 */
package com.Nutch.Crawl.TelAmz;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
//import java.util.Scanner;

/**
 * @author surendra
 *
 */
public class FileStore {

	/**
	 * 
	 */
	public FileStore() {
		// TODO Auto-generated constructor stub
	}
	
	
	final  static String filePath ="/katta/TeleAmazonas";
	final  static String filename="TeleAmazon_Terminal_";
	static File fileM=null;
	static File fileMRAT=null;
	static File fileC=null;
	static File filePC=null;
	static File fileRM=null;
	static File filePR=null;
	static File fileCA=null;
	static File fileTvshow=null;
	static File fileTvshowEps=null;
	static File fileTHR=null;
	static File fileTHRAVL=null;
	static File fileRPG=null;
	static File fileOL=null;
	static File fileSched=null;
	static File fileChanles=null;
	
	//static int count=0;
	/*
	final static String movietable="movie_";
	final static String crewtable="crew_";
	final static String programcrew="programcrew_";
	final static String richmediatable="richmedia_";
	final static String programrelease="release_";
	
	
	public static String GetCurrentTimeStamp1() {
        SimpleDateFormat sdfDate = new SimpleDateFormat(
                "yyyyMMdd'T'HHmmssSSSSS");// dd/MM/yyyy
        Date now = new Date();
        String strDate = sdfDate.format(now);
        return strDate;
    }
	 */
	
	/////////////////////////////////////////////// Movie Table///////////////////////////////
	public static void MovieTable(String table) {
        //get current project path
      
        //create a new file with Time Stamp
        fileM = new File(filePath + "/" + filename+table+"_"+GetCurrentTimeStamp().replace(":","").replace(".","")+".queries");
        
       
        try {
        	  if (!fileM.exists()) {
                  fileM.createNewFile();
                 // System.out.println("File is created; file name is " + fileM.getName());
                  
                  
                   
                  
                 
              } else {
            	  
            	   // System.out.println("File already exist");
              }
        	  
         
        	
        	 
           } catch (IOException e) {
            e.printStackTrace();
        }
    }
	
	//////////////////////////////////////////////////  TVSHOWS//////////////////////////////
	
	
	public static void TVShowTable(String table) {
        //get current project path
       // String filePath = ;
        //create a new file with Time Stamp
        fileTvshow = new File(filePath + "/" + filename+table+"_"+GetCurrentTimeStamp().replace(":","").replace(".","")+".queries");

        try {
            if (!fileTvshow.exists()) {
                fileTvshow.createNewFile();
                //System.out.println("File is created; file name is " + fileTvshow.getName());
            } else {
                //System.out.println("File already exist");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

	//////////////////////////////////////////////////  EPSIODES////////////////////////////
	
	public static void EpsiodeTable(String table) {
        //get current project path
       // String filePath = ;
        //create a new file with Time Stamp
        fileTvshowEps = new File(filePath + "/" + filename+table+"_"+GetCurrentTimeStamp().replace(":","").replace(".","")+".queries");

        try {
            if (!fileTvshowEps.exists()) {
                fileTvshowEps.createNewFile();
                //System.out.println("File is created; file name is " + fileTvshowEps.getName());
            } else {
                //System.out.println("File already exist");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

	
	/////////////////////////////////////////////////////////////////////// ProgramRelease///////////////
	
	
	public static void ProgramReleaseTable(String table) {
        //get current project path
       // String filePath = ;
        //create a new file with Time Stamp
        filePR = new File(filePath + "/" + filename+table+"_"+GetCurrentTimeStamp().replace(":","").replace(".","")+".queries");

        try {
            if (!filePR.exists()) {
                filePR.createNewFile();
               // System.out.println("File is created; file name is " + filePR.getName());
            } else {
                //System.out.println("File already exist");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

	//////////////////////////////////////ProgramCrew//////////////////////////
	
	
	
	public static void ProgramCrewTable(String table) {
        //get current project path
       // String filePath = ;
        //create a new file with Time Stamp
        filePC = new File(filePath + "/" + filename+table+"_"+GetCurrentTimeStamp().replace(":","").replace(".","")+".queries");

        try {
            if (!filePC.exists()) {
                filePC.createNewFile();
                //System.out.println("File is created; file name is " + filePC.getName());
            } else {
                //System.out.println("File already exist");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
////////////////////////////////////////////////////////////////Crew///////////////////////////////
	
	public static void CrewTable(String table) {
        //get current project path
       // String filePath = ;
        //create a new file with Time Stamp
        fileC= new File(filePath + "/" + filename+table+"_"+GetCurrentTimeStamp().replace(":","").replace(".","")+".queries");

        try {
            if (!fileC.exists()) {
                fileC.createNewFile();
                //System.out.println("File is created; file name is " + fileC.getName());
            } else {
                //System.out.println("File already exist");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

	
	////////////////////////////////////////RichMedia//////////////////////////
	
	public static void RichMediaTable(String table) {
        //get current project path
       // String filePath = ;
        //create a new file with Time Stamp
        fileRM= new File(filePath + "/" + filename+table+"_"+GetCurrentTimeStamp().replace(":","").replace(".","")+".queries");

        try {
            if (!fileRM.exists()) {
                fileRM.createNewFile();
                //System.out.println("File is created; file name is " + fileRM.getName());
            } else {
                //System.out.println("File already exist");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


////////////////////////////////  Awards Table///////////////////////////
	
	
	public static void AwardsTable(String table) {
        //get current project path
       // String filePath = ;
        //create a new file with Time Stamp
        fileCA= new File(filePath + "/" + filename+table+"_"+GetCurrentTimeStamp().replace(":","").replace(".","")+".queries");

        try {
            if (!fileCA.exists()) {
                fileCA.createNewFile();
                //System.out.println("File is created; file name is " + fileCA.getName());
            } else {
                //System.out.println("File already exist");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//////////////////////////////////////// Rating Table///////////////////////////
	
	
	public static void RatingTable(String table) {
        //get current project path
       // String filePath = ;
        //create a new file with Time Stamp
        fileMRAT= new File(filePath + "/" + filename+table+"_"+GetCurrentTimeStamp().replace(":","").replace(".","")+".queries");

        try {
            if (!fileMRAT.exists()) {
                fileMRAT.createNewFile();
                //System.out.println("File is created; file name is " + fileMRAT.getName());
            } else {
              //  System.out.println("File already exist");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

	
	
	
	
///////////////////////////////////////////////////////////////////	Theater Table////////////////////////////////////////////
	
	
	public static void TheaterTable(String table) {
        //get current project path
       // String filePath = ;
        //create a new file with Time Stamp
        fileTHR= new File(filePath + "/" + filename+table+"_"+GetCurrentTimeStamp().replace(":","").replace(".","")+".queries");

        try {
            if (!fileTHR.exists()) {
                fileTHR.createNewFile();
                //System.out.println("File is created; file name is " + fileMRAT.getName());
            } else {
              //  System.out.println("File already exist");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

///////////////////////////////////////////////////////////////////	TheaterAvailability Table////////////////////////////////////////////
	
	public static void TheaterAvailTable(String table) {
        //get current project path
       // String filePath = ;
        //create a new file with Time Stamp
        fileTHRAVL= new File(filePath + "/" + filename+table+"_"+GetCurrentTimeStamp().replace(":","").replace(".","")+".queries");

        try {
            if (!fileTHRAVL.exists()) {
                fileTHRAVL.createNewFile();
                //System.out.println("File is created; file name is " + fileMRAT.getName());
            } else {
              //  System.out.println("File already exist");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
	
//////////////////////////////////////////////// Related Programs/////////////////////////////	
	
	
	public static void RelatedPrgTable(String table) {
        //get current project path
       // String filePath = ;
        //create a new file with Time Stamp
        fileRPG= new File(filePath + "/" + filename+table+"_"+GetCurrentTimeStamp().replace(":","").replace(".","")+".queries");

        try {
            if (!fileRPG.exists()) {
                fileRPG.createNewFile();
                //System.out.println("File is created; file name is " + fileMRAT.getName());
            } else {
              //  System.out.println("File already exist");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

	
//////////////////////////////////////////////// Other Links/////////////////////////////	
	
	
	public static void OtherLinksTable(String table) {
        //get current project path
       // String filePath = ;
        //create a new file with Time Stamp
        fileOL= new File(filePath + "/" + filename+table+"_"+GetCurrentTimeStamp().replace(":","").replace(".","")+".queries");

        try {
            if (!fileOL.exists()) {
                fileOL.createNewFile();
                //System.out.println("File is created; file name is " + fileMRAT.getName());
            } else {
              //  System.out.println("File already exist");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
//////////////////////////////////////////////// Channels Table/////////////////////////////	
	
	
	public static void ChannelsTable(String table) {
        //get current project path
       // String filePath = ;
        //create a new file with Time Stamp
        fileChanles= new File(filePath + "/" + filename+table+"_"+GetCurrentTimeStamp().replace(":","").replace(".","")+".queries");

        try {
            if (!fileChanles.exists()) {
                fileChanles.createNewFile();
                //System.out.println("File is created; file name is " + fileMRAT.getName());
            } else {
              //  System.out.println("File already exist");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
	
	
	
	
//////////////////////////////////////////////// SchedulesTable/////////////////////////////	
	
	
	public static void SchedulesTable(String table) {
        //get current project path
       // String filePath = ;
        //create a new file with Time Stamp
        fileSched= new File(filePath + "/" + filename+table+"_"+GetCurrentTimeStamp().replace(":","").replace(".","")+".queries");

        try {
            if (!fileSched.exists()) {
                fileSched.createNewFile();
                //System.out.println("File is created; file name is " + fileMRAT.getName());
            } else {
              //  System.out.println("File already exist");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }




	
	////////////////////////////////////////////////////////////  GET SYSTEM TIME ////////////////////////////////////////////
	
	
        // Get current system time
    public static String GetCurrentTimeStamp() {
        SimpleDateFormat sdfDate = new SimpleDateFormat(
                "yyyyMMdd'T'HH:mm:ss.SSSSS");// dd/MM/yyyy
        Date now = new Date();
        String strDate = sdfDate.format(now);
        return strDate;
    }
    
    
    
    /*
    // Get Current Host Name
    public static String GetCurrentTestHostName() throws UnknownHostException {
        InetAddress localMachine = InetAddress.getLocalHost();
        String hostName = localMachine.getHostName();
        return hostName;
    }

    // Get Current User Name
    public static String GetCurrentTestUserName() {
        return System.getProperty("user.name");
    }
    
*/
	 


}