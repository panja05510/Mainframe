package com.citizens.mainframe.service;

import java.io.IOException;
import java.nio.charset.Charset;

import org.springframework.stereotype.Service;

@Service
public class Convertions {

	public byte[] jsonToEbcdic(Object object) throws IOException {
//      String jsonString = new ObjectMapper().writeValueAsString(jsonObject);
		String objectToString=object.toString();
      return objectToString.getBytes(Charset.forName("IBM1047")); 
  }
	
	public String ebcdicToJson(byte[] ebcdicData) throws IOException {
      String jsonData = new String(ebcdicData, Charset.forName("IBM1047")); 
//      JsonParser jsonParser = new JsonParser();
//		JsonObject jsonObject = jsonParser.parse(jsonData).getAsJsonObject();
      return jsonData;
  }
}
