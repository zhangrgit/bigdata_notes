package com.hf;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.codec.binary.Base64;
 
public class KylinBuild {
	

	static String ACCOUNT = "KYLIN";
    static String PWD = "KYLIN";
    static String PATH = "http://ip:7070/kylin/api/cubes/XXX_CUBE/rebuild";

    
    static SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd 00:00:00");
 
    public static void main(String[] args) {
    	   try {
    		System.out.println(sdf.format(new Date()));
			long start=sdf.parse(sdf.format(new Date())).getTime();
			long end=start+24*60*60*1000;
			System.out.println(start+","+end);
			Put(PATH,"{\"startTime\": "+start+",\"endTime\": "+end+",\"buildType\": \"BUILD\"}");
		} catch (ParseException e) {
			e.printStackTrace();
		}
    }
 
    public static String Put(String addr, String params) {
        String result = "";
        try {
            URL url = new URL(addr);
            HttpURLConnection connection = (HttpURLConnection) url
                    .openConnection();
            connection.setRequestMethod("PUT");
            connection.setDoOutput(true);
            String auth = ACCOUNT + ":" + PWD;
            String code = new String(new Base64().encode(auth.getBytes()));
            connection.setRequestProperty("Authorization", "Basic " + code);
            connection.setRequestProperty("Content-Type",
                    "application/json;charset=UTF-8");
            PrintWriter out = new PrintWriter(connection.getOutputStream());
            out.write(params);
            out.close();
            BufferedReader in;
            try {
                in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            } catch (FileNotFoundException exception) {
                InputStream err = ((HttpURLConnection) connection).getErrorStream();
                if (err == null)
                    throw exception;
                    in = new BufferedReader(new InputStreamReader(err));
            }
            StringBuffer response = new StringBuffer();
            String line;
            while ((line = in.readLine()) != null)
                response.append(line + "\n");
            in.close();
 
            result = response.toString();
            System.err.println(result);
        } catch (MalformedURLException e) {
            System.err.println("MalformedURLException:"+e.toString());
        } catch (IOException e) {
            System.err.println("IOException:"+e.toString());
        }
        return result;
    }
}