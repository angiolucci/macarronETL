package generic;

import java.util.HashMap;

public class DBConfigReader {
	private FileHandler configFileHandler = null;

	public DBConfigReader(String filePath) {
		try {
			this.configFileHandler = new FileHandler(filePath);
		} catch (RuntimeException rte){
			// FILE NOT FOUND, JUST SET NULL AND USE DEFAULT VALUES
			// FOR THE DB CONNECTION
			this.configFileHandler = null;
		}
	}

	public HashMap<String, String> getConnProperties() {
		String readenData = null;
		String[] keyValuePairs = null;
		HashMap<String, String> properties = new HashMap<String, String>();
		
		properties.put("connection", "jdbc:postgresql://localhost/dw_proto2");
		properties.put("user", "postgres");
		properties.put("password", " ");

		
		if (this.configFileHandler != null){
			while ((readenData = configFileHandler.readLine()) != null) {
				// Ignora comentários
				if (!readenData.startsWith("#")) {

					keyValuePairs = readenData.split("=");

					// Adiciona apenas se conseguiu dar parsing em duas strings
					if (keyValuePairs.length == 2){
						
						String key = keyValuePairs[0].trim().toLowerCase();
						String val = keyValuePairs[1].trim();
						
						if ( key.equalsIgnoreCase("connection") ){
							val = "jdbc:postgresql://" + val;
						}
							
						properties.put(key, val);
					}
				}
			}	
		}
		String[] showparams = properties.get("connection").split("/");
		System.out.print("Using database " + showparams[showparams.length-1]);
		System.out.println(" at server " + showparams[showparams.length-2]);
		return properties;
	}
}
