package org.isobit.util;

public class XUtil {
	
    public static int intValue(Object value) {    	
        if (value instanceof Tag) {
            value = ((Tag) value).getId();
        }
        try {
            return value instanceof Integer ? (Integer) value : (value instanceof Number ? ((Number) value).intValue()
                    : (int) Double.parseDouble(value + ""));
        } catch (Exception ex) {
            return 0;
        }
    }
    
    public static boolean isEmpty(Object value) {
        return value == null || value.toString().trim().length() == 0;
    }
    
    public static String implode(Object[] values) {
        return implode(values, ',');
    }

    public static String implode(Object[] values, Object delimiter) {
        return values!=null?implode(values, delimiter, values.length):"";
    }

    public static String implode(Object[] values, Object delimiter, int limit) {
        StringBuilder builder = new StringBuilder(values.length > 0 && values[0] != null ? values[0].toString() : "");
        for (int i = 1; i < limit; i++) {
            if (values[i] != null) {
                builder.append(delimiter).append(values[i]);
            }
        }
        return builder.toString();
    }

}
