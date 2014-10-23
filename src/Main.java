import generic.DBPopulator;

public class Main {
	public static void main(String[] args){
		try {
			System.out.println("\nPopulate DB using:\n" + args[0] + "\n");
			new DBPopulator(args[0]);
		} catch (IndexOutOfBoundsException e) {
			System.err.println("Você deve especificar um arquivo de origem!");
			System.exit(-1);
		}
	}
}
