import generic.Populator;

public class Main {
	public static void main(String[] args){
		try {
			
			//for (int i = 0; i < args.length; i++){
				System.out.println("\nPopulate DB using:\n" + args[0] + "\n");
				new Populator(args[0]);
			//}
			
		} catch (IndexOutOfBoundsException e) {
			System.err.println("Você deve especificar um arquivo de origem!");
			System.exit(-1);
		}

	}
}
