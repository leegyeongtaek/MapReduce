package com.client.hdfs.common;

public enum Country {

	Australia {
		public String countryName() {return "Australia";}
	},
	Brazil {
		public String countryName() {return "Brazil";}
	},
	France {
		public String countryName() {return "France";}
	}
	;
	
	public abstract String countryName();
	
	public static Country getCountry(String s) {
	      for (Country c : values()) {
	    	  
	        // iteration order is in decl order, so SECONDS matched last
	        if (s.startsWith(c.countryName())) {
	          return c;
	        }
	      }
	      return null;
	    }
	
}
