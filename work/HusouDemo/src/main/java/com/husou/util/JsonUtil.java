package com.husou.util;

import java.io.StringWriter;
import java.io.Writer;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtil {

    private static ObjectMapper jasonMapper = new ObjectMapper();
    
    public static Map<String, Object> deserialize(String jsonStr) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> data = jasonMapper.readValue(jsonStr, Map.class);
            return data;
        } catch (Exception e) {
            throw new java.lang.IllegalArgumentException(e);
        }
    }
    
    public static <T extends Object> T deserialize(String jsonStr, Class<T> typeClass) {
        try {
            T data = jasonMapper.readValue(jsonStr, typeClass);
            return data;
        } catch (Exception e) {
            throw new java.lang.IllegalArgumentException(e);
        }
    }
    
    public static String serialize(Object data) {
        Writer w = new StringWriter();
        try {
            jasonMapper.writeValue(w, data);
        } catch (Exception e) {
            throw new java.lang.IllegalArgumentException(e);
        }
        return w.toString();
    }
    
    public static String prettyPrint(String jsonStr){
        String result = "";
        try {
            Object json = jasonMapper.readValue(jsonStr, Object.class);
            result = jasonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
        } catch (Exception e) {
            e.printStackTrace();
        } 
        
        return result;
    }
    
    public static String prettyPrint(Object json){
        String result = "";
        try {
            result = jasonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
        } catch (Exception e) {
            e.printStackTrace();
        } 
        
        return result;
    }
}
