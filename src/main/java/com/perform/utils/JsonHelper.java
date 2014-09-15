package com.perform.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;


public class JsonHelper {
	public static final String EMPTY = "{}";
	private static final ObjectMapper mapper;
	private static final ObjectMapper objMap;
	private static final JsonFactory factory;

	static {
		objMap = new ObjectMapper();
		factory = new JsonFactory(); 		
		mapper = new ObjectMapper(factory); 

	}


	
	public static <T> List<T> getFromJSON(final String jsonString, final Class<T> type) throws IOException {
	    return new ArrayList<T>() {{ add(objMap.readValue(jsonString, type));}};
	  }


	public static <T> List<T> getFromJSONCollection(String jsonString, final Class<T> type) throws IOException {
		try {
			return objMap.readValue(jsonString, TypeFactory.defaultInstance().constructCollectionType(ArrayList.class, type));
		} catch (JsonMappingException e) {
			return getFromJSON(jsonString, type);
		}
	}
	
	public static HashMap<String, Object> getHashMapFromJson(String jsonStr) throws JsonParseException, JsonMappingException, IOException {

		TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {};

		HashMap<String, Object> hmap = mapper.readValue(factory.createParser(jsonStr.getBytes()), typeRef);		
		return hmap;	
	}
	
	public static void main(String[] s) throws JsonParseException, JsonMappingException, IOException {
		List<HashMap> t = getFromJSONCollection("[{\"a\":1},{\"a\":2}]", HashMap.class);
		System.out.println("test");
		
	}



	
	/*
	public static boolean isValidJSON(final String json) {
		   boolean valid = false;
		   try {
		      final JsonParser parser = new ObjectMapper().getJsonFactory()
		            .createJsonParser(json);
		      while (parser.nextToken() != null) {
		      }
		      valid = true;
		   } catch (JsonParseException jpe) {
		      throw new CTCException(jpe.getMessage(), jpe);
		   } catch (IOException ioe) {
			   throw new CTCException(ioe.getMessage(), ioe);
		   }
		 
		   return valid;
		}*/
}
