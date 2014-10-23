import generic.DBPopulator;

public class Main {
	public static void main(String[] args){
		try {
			System.out.println("\nPopulate DB using:\n" + args[0] + "\n");
		} catch (IndexOutOfBoundsException e) {
			System.err.println("Você deve especificar um arquivo de origem!");
			System.out.println(e.getMessage());
			e.printStackTrace();
			System.exit(-1);
		}
		
		new DBPopulator(args[0]);
	}
}
