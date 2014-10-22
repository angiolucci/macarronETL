package generic;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Random;

public class DBPopulator {
	public DBPopulator(String csv_file_path) {
		long totalRecords = 0;
		long totalInvalids = 0;

		FileHandler fh = new FileHandler(csv_file_path);
		String line;
		HashMap<String, Integer> fields = new HashMap<String, Integer>();
		HashMap<Integer, String> data = new HashMap<Integer, String>();

		line = fh.readLine();
		String[] field_names = line.split(",");
		for (int i = 0; i < field_names.length; i++)
			fields.put(field_names[i], i);

		while ((line = fh.readLine()) != null) {
			Long m_id = null;
			Long o_id = null;
			Long s_id = null;
			Long t_id = null;
			Long d_id = null;
			Long ccn = null;

			String regex = ",(?!\\sCUTTING)";
			String[] data_values = line.split(regex);
			for (int i = 0; i < data_values.length; i++) {
				data.put(i, data_values[i]);
			}
			
			
			/*
			* Se o número de campos do registro for diferente
			* do esperado, ignora o registro
			*/
			if (field_names.length != data_values.length){
				System.out.println(field_names.length + " " + data_values.length);
				System.err.println("Ignorando registro " + (++totalRecords) + " com campos inválidos");
				totalInvalids++;
				continue;
			}

			/* TABLE METHOD */
			/*
			 * ------------------------------------------------------------------
			 */
			
			// Padroniza as várias notações para um mesmo METHOD
			String M_METHOD = (String) data.get((Integer) fields.get("METHOD"));
			if (M_METHOD.contains("KNIFE"))
				M_METHOD = "KNIFE  CUTTING INSTR";
			else if (M_METHOD.equalsIgnoreCase("1"))
				M_METHOD = "BURGLARY 1";
			else if (M_METHOD.equalsIgnoreCase("2"))
				M_METHOD = "BURGLARY 2";
			else if (M_METHOD.equalsIgnoreCase("BFT"))
				M_METHOD = "BLUNT FORCE TRAUMA";
			else if (M_METHOD.equalsIgnoreCase("ARS"))
				M_METHOD = "ARSON";
			
			String select_m = new String("SELECT ID FROM METHOD WHERE "
					+ "INFO = '"
					+ M_METHOD + "'");
			m_id = DBConn.executeQuery(select_m, "id");

			if (m_id == null) {
				String insert_m = new String("INSERT INTO METHOD (" + "INFO"
						+ ") VALUES (\'"
						+ M_METHOD
						+ "\');");

				DBConn.insertRecord(insert_m);
				m_id = DBConn.executeQuery(select_m, "id");
			}

			/*
			 * ------------------------------------------------------------------
			 */

			/* TABLE TIME */
			/*
			 * ------------------------------------------------------------------
			 */

			HashMap<Integer, String> monthName = new HashMap<Integer, String>();
			monthName.put(1, "JANUARY");
			monthName.put(2, "FEBRUARY");
			monthName.put(3, "MARCH");
			monthName.put(4, "APRIL");
			monthName.put(5, "MAY");
			monthName.put(6, "JUNE");
			monthName.put(7, "JULY");
			monthName.put(8, "AUGUST");
			monthName.put(9, "SEPTEMBER");
			monthName.put(10, "OCTOBER");
			monthName.put(11, "NOVEMBER");
			monthName.put(12, "DECEMBER");

			HashMap<Integer, String> weekDayName = new HashMap<Integer, String>();
			weekDayName.put(1, "SUNDAY");
			weekDayName.put(2, "MONDAY");
			weekDayName.put(3, "TUESDAY");
			weekDayName.put(4, "WEDNESDAY");
			weekDayName.put(5, "THURSDAY");
			weekDayName.put(6, "FRIDAY");
			weekDayName.put(7, "SATURDAY");

			String strDateTmp[] = ((String) data.get((Integer) fields
					.get("REPORTDATETIME"))).split(" ");
			strDateTmp = strDateTmp[0].split("/");

			Calendar c = Calendar.getInstance();
			c.clear();
			c.set(Integer.parseInt(strDateTmp[2]),
					Integer.parseInt(strDateTmp[0]) - 1,
					Integer.parseInt(strDateTmp[1]));
			int dayOfWeek = c.get(Calendar.DAY_OF_WEEK);

			String select_t = new String("SELECT ID FROM TIME WHERE "
					+ "MONTH = "
					+ strDateTmp[0]
					+ " AND DAY = "
					+ strDateTmp[1]
					+ " AND YEAR = "
					+ strDateTmp[2]
					+ " AND SHIFT = '"
					+ (String) data.get((Integer) fields.get("SHIFT"))
							.substring(0, 3) + "'");

			t_id = DBConn.executeQuery(select_t, "id");
			if (t_id == null) {
				String insert_t = new String("INSERT INTO TIME ("
						+ "MONTH, DAY, YEAR, SHIFT, MONTH_CHAR, DAY_OF_WEEK,"
						+ "DAY_OF_WEEK_CHAR"
						+ ") VALUES ("
						+ strDateTmp[0]
						+ ", "
						+ strDateTmp[1]
						+ ", "
						+ strDateTmp[2]
						+ ", '"
						+ (String) data.get((Integer) fields.get("SHIFT"))
								.substring(0, 3)
						+ "', "
						+ "'"
						+ (String) monthName.get(Integer
								.parseInt(strDateTmp[0])) + "', " + dayOfWeek
						+ ", " + "'" + (String) weekDayName.get(dayOfWeek)
						+ "'" + ");");
				DBConn.insertRecord(insert_t);
				t_id = DBConn.executeQuery(select_t, "id");
			}

			/*
			 * ------------------------------------------------------------------
			 */

			/* TABLE OFFENSE */
			/*
			 * ------------------------------------------------------------------
			 */
			
			String O_OFFENSE = (String) data.get((Integer) fields.get("OFFENSE"));
			
			if (O_OFFENSE.equalsIgnoreCase("ARS"))
				O_OFFENSE = "ARSON";
			
			if (O_OFFENSE.equalsIgnoreCase("ADW"))
				O_OFFENSE = "ASSAULT W/DANGEROUS WEAPON";
			
			String select_o = new String("SELECT ID FROM OFFENSE WHERE "
					+ " INFO = '"
					+ O_OFFENSE + "'");

			o_id = DBConn.executeQuery(select_o, "id");
			if (o_id == null) {
				String insert_o = new String("INSERT INTO OFFENSE (" + "INFO"
						+ ") VALUES (\'"
						+ O_OFFENSE
						+ "\');");
				DBConn.insertRecord(insert_o);
				o_id = DBConn.executeQuery(select_o, "id");

			}

			/*
			 * ------------------------------------------------------------------
			 */

			/* TABLE DISTRICT */
			/*
			 * ------------------------------------------------------------------
			 */
			
			// Trata quando o valor do campo distrito está em branco
			String D_DISTRICT = (String) data.get((Integer) fields.get("DISTRICT"));
			if (D_DISTRICT.trim().isEmpty())
				D_DISTRICT = "NONE";
			
			String select_d = new String("SELECT ID FROM DISTRICT WHERE "
					+ "INFO = '"
					+ D_DISTRICT + "'");

			d_id = DBConn.executeQuery(select_d, "id");
			if (d_id == null) {
				String insert_d = new String("INSERT INTO DISTRICT (" + "INFO"
						+ ") VALUES (\'"
						+ D_DISTRICT
						+ "\');");

				DBConn.insertRecord(insert_d);
				d_id = DBConn.executeQuery(select_d, "id");
			}

			/*
			 * ------------------------------------------------------------------
			 */

			/* TABLE SITE */
			/*
			 * ------------------------------------------------------------------
			 */

			// Trata caracteres inválidos no endereço
			String S_ADDRESS = (String) data.get((Integer) fields
					.get("BLOCKSITEADDRESS"));
			S_ADDRESS = S_ADDRESS.replace('\'', ' ');
			S_ADDRESS = S_ADDRESS.replace('\"', ' ');
			
			if (S_ADDRESS.trim().isEmpty())
				S_ADDRESS = "NONE";

			// Trata número de WARDs inválidos
			String S_WARD = (String) data.get((Integer) fields.get("WARD"));
			try {
				Integer.parseInt(S_WARD);
			} catch (NumberFormatException ne) {
				S_WARD = "0";
			}

			// Trata número de clusters fora do padrão
			String S_CLUSTER = (String) data.get((Integer) fields
					.get("NEIGHBORHOODCLUSTER"));

			try {
				Integer.parseInt(S_CLUSTER);
			} catch (NumberFormatException ne) {
				S_CLUSTER = "0";
			}

			// Trata números de PSAs fora do padrão
			String S_PSA = (String) data.get((Integer) fields.get("PSA"));
			try {
				Integer.parseInt(S_PSA);
			} catch (NumberFormatException ne) {
				S_PSA = "0";
			}

			String local_tmp = "NULL";
			String[] region_tmp = ((String) data.get((Integer) fields
					.get("BLOCKSITEADDRESS"))).split(" ");

			if (region_tmp[region_tmp.length - 1].equalsIgnoreCase("N")
					|| region_tmp[region_tmp.length - 1].equalsIgnoreCase("S")
					|| region_tmp[region_tmp.length - 1].equalsIgnoreCase("W")
					|| region_tmp[region_tmp.length - 1].equalsIgnoreCase("E")
					|| region_tmp[region_tmp.length - 1]
							.equalsIgnoreCase("NNE")
					|| region_tmp[region_tmp.length - 1].equalsIgnoreCase("NE")
					|| region_tmp[region_tmp.length - 1]
							.equalsIgnoreCase("ENE")
					|| region_tmp[region_tmp.length - 1]
							.equalsIgnoreCase("ESE")
					|| region_tmp[region_tmp.length - 1].equalsIgnoreCase("SE")
					|| region_tmp[region_tmp.length - 1]
							.equalsIgnoreCase("SSE")
					|| region_tmp[region_tmp.length - 1]
							.equalsIgnoreCase("SSW")
					|| region_tmp[region_tmp.length - 1].equalsIgnoreCase("SW")
					|| region_tmp[region_tmp.length - 1]
							.equalsIgnoreCase("WSW")
					|| region_tmp[region_tmp.length - 1]
							.equalsIgnoreCase("WNW")
					|| region_tmp[region_tmp.length - 1].equalsIgnoreCase("NW")
					|| region_tmp[region_tmp.length - 1]
							.equalsIgnoreCase("NNW"))
				local_tmp = "'" + region_tmp[region_tmp.length - 1] + "'";

			String select_s = new String("SELECT ID FROM SITE WHERE "
					+ "ADDRESS = '" + S_ADDRESS + "'" + " AND CLUSTER = "
					+ S_CLUSTER + " AND PSA = " + S_PSA + " AND WARD = "
					+ S_WARD);

			s_id = DBConn.executeQuery(select_s, "id");
			if (s_id == null) {
				String insert_s = new String("INSERT INTO SITE ("
						+ "ADDRESS, CLUSTER, PSA, WARD, REGION"
						+ ") VALUES (\'" + S_ADDRESS + "\', " + S_CLUSTER
						+ ", " + S_PSA + ", " + S_WARD + ", " + local_tmp
						+ ");");

				DBConn.insertRecord(insert_s);
				s_id = DBConn.executeQuery(select_s, "id");

			}

			/*
			 * ------------------------------------------------------------------
			 */

			/*
			 * ----------------------------------------------------------------
			 * TABLE FACT
			 * ----------------------------------------------------------------
			 */

			// Existem algumas tabelas fora de padrão, que adotam campos
			// diferentes como chave (CCN ou NID). Para o caso onde houverem
			// anomalias onde não há chave presente, gera uma chave com base
			// em um número aleatório e nas chaves-estrangeiras.
			String F_CCN = (String) data.get((Integer) fields.get("CCN"));
			try {
				Integer.parseInt(F_CCN);
			} catch (NumberFormatException ne) {
				try {
					F_CCN = (String) data.get((Integer) fields.get("NID"));
					Integer.parseInt(F_CCN);
				} catch (NumberFormatException ne2) {
					Random randomGenerator = new Random();
					Integer generatedKey = randomGenerator.nextInt(10000);
					F_CCN = "0" + generatedKey + m_id + o_id + s_id + t_id
							+ d_id;

				}
			}

			String select_f = new String("SELECT CCN FROM FACT WHERE "
					+ "M_ID = " + m_id + " AND O_ID = " + o_id + " AND S_ID = "
					+ s_id + " AND T_ID  = " + t_id + " AND D_ID  = " + d_id);

			ccn = DBConn.executeQuery(select_f, "ccn");

			if (ccn == null) {
				String insert_f = new String("INSERT INTO FACT (" + "CCN"
						+ " ,M_ID, O_ID, S_ID, " + "T_ID, D_ID) " + "VALUES ("
						+ F_CCN + ", " + m_id + ", " + o_id + ", " + s_id
						+ ", " + t_id + ", " + d_id + ")");
				DBConn.insertRecord(insert_f);
				ccn = DBConn.executeQuery(select_f, "ccn");
				totalRecords++;
			}
			System.out.println("Processed record no. " + totalRecords);

		}

		System.out.println(totalRecords + " records were processed!");
		System.out.println(totalInvalids + " records were dropped out!");
	}
}
