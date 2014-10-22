package generic;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

public class FileHandler {
	private	static File file;
	private static BufferedReader bf;
		
	
	public FileHandler(String filePath){
		file = new File(filePath);
		bf = null;
		try {
			bf = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e.getMessage());
		}

	}
	
	public String readLine(){
		try {
			return bf.readLine();
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage());
		}
	}
	
	public void write(String filePath, String buffer){
		PrintWriter pw = null;
		try {
			pw = new PrintWriter(filePath);
			pw.println(buffer);
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Arquivo não encontrado");
		}
		finally {
			pw.close();
		}
	}
}
