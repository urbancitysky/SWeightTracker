package actWeight;

import java.awt.Component;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.SwingUtilities;

import java.text.SimpleDateFormat;

public class ActWeight {

	static String CSServer = "";
	static String CSacc = "";
	static String CSpw = "";

	static String FTPServer = "jdbc:mysql://localhost:3306/sweight";
	static String FTPacc = "root";
	static String FTPpw = "root";

	static int itemCol = 1;
	static int weightCol = 2;

	public static void main(String args[]) throws SQLException {

		try {
			long startTime = System.nanoTime();
			// connect servers
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			Connection CSconn = DriverManager.getConnection(CSServer, CSacc, CSpw);
			Connection FTPconn = DriverManager.getConnection(FTPServer, FTPacc, FTPpw);

			if (FTPconn != null && CSconn != null) {
				System.out.println("Both MySQL servers are connected");

				// retrieving data from CS server
				Statement CSstmt = CSconn.createStatement();
				String CSquery = "SELECT Itemno1, min(Sweight) FROM aisdatax.tshippex t "
						+ "group by Itemno1 asc order by Itemno1 asc;";
				ResultSet CSrs = CSstmt.executeQuery(CSquery);

				// retrieving data from FTP server
				// System.out.println("\nMySQL servers FTP result ");
				Statement FTPstmt = FTPconn.createStatement();
				String FTPquery = "SELECT Itemno1, Sweight FROM k2aw.awdata a "
						+ "group by Itemno1 asc order by Itemno1 asc;";
				ResultSet FTPrs = FTPstmt.executeQuery(FTPquery);

				int CSRows = countRow(CSrs);
				int FTPRows = countRow(FTPrs);
				System.out.print("CS rows: " + CSRows + "\n");
				System.out.println("FTP rows: " + FTPRows + "\n");

				List<String> FTPitem = new ArrayList<String>();
				List<String> FTPItemWeight = new ArrayList<String>();
				while (FTPrs.next()) {
					FTPitem.add(FTPrs.getString(itemCol));
					FTPItemWeight.add(FTPrs.getString(weightCol));
				}

				if (FTPRows == 0) {
					Statement AddFTPstmt = FTPconn.createStatement();
					String queryAdd = "INSERT INTO `k2aw`.`awdata` (`Itemno1`, `Sweight`) " + "VALUES ('test', '1');";
					AddFTPstmt.execute(queryAdd);
					FTPRows = FTPRows + 1;
					System.out.println("new FTP rows: " + FTPRows + "\n");
				}

				// check if exists in FTP table
				// false --> add item
				// outer loop: CS rows
				// inner loop: FTP rows
				ProgressBar pbar = new ProgressBar();

				pbar.setVisible(true);

				int percent;
				int matchCount = 0, updateCount = 0, doNothing = 0, addCount = 0, pointer = 1;
				while (CSrs.next()) {
					boolean found = false;
					for (int i = 0; i <= FTPRows - 1; i++) {
						String aa = CSrs.getString(itemCol);
						String bb = FTPitem.get(i);
						double cc = Double.parseDouble(CSrs.getString(weightCol));
						double dd = Double.parseDouble(FTPItemWeight.get(i));
						if (aa.equals(bb)) {
							System.out.print("hit!! ");
							found = true;
							matchCount = matchCount + 1;
							// compare items weight
							// CS < FTP -->update weight
							if (cc < dd) {
								System.out.println("lighter, go ahead update weight");
								updateCount = updateCount + 1;
								System.out.println(aa + "\t" + cc + "\n" + bb + "\t" + dd);
								// System.out.println(queryUPDATE+ "\n");
								Statement updateFTPstmt = FTPconn.createStatement();
								String queryUPDATE = "UPDATE `awdata` SET `Sweight` = " + cc + " WHERE `Itemno1` = '"
										+ aa + "';";
								updateFTPstmt.execute(queryUPDATE);
							} else if (cc > dd) {
								// CS >= FTP do nothing
								System.out.println("but heavier, do nothing");
								doNothing++;
							} else {
								System.out.println("but the same weight, do nothing");
								doNothing++;
							}
						} else {
							// not found and the last item --> add to fTP
							System.out.print("no match, ");
							if (i == FTPRows - 1 && found == false) {
								System.out.println("last item in FTP, not found, add item\n");
								Statement AddFTPstmt = FTPconn.createStatement();
								String queryAdd = "INSERT INTO `awdata` (`Itemno1`, `Sweight`) " + "VALUES ('" + aa
										+ "', '" + cc + "');";
								AddFTPstmt.execute(queryAdd);
								addCount = addCount + 1;
							} else {
								// not found and not the last item --> continue
								System.out.println("continue");
							}
						}
						System.out.println(aa + "\t\t" + cc + "\n" + bb + "\t\t" + dd + "\n");
					}
					pointer++;
					percent = 100 * pointer / CSRows;
					System.out.println("percent: " + percent);
					pbar.update(percent);
					// System.out.println(percent);
				}
				pbar.setVisible(false);//
				pbar.dispose();

				System.out.println("\nmatch: " + matchCount + "\nupdate: " + updateCount + "\ndo nothing: " + doNothing
						+ "\nadd: " + addCount + "\n");

				Statement Exstmt = FTPconn.createStatement();
				String Exquery = "SELECT Itemno1, Sweight FROM k2aw.awdata a "
						+ "group by Itemno1 asc order by Itemno1 asc;";
				ResultSet Exrs = FTPstmt.executeQuery(Exquery);

				export(Exrs);
				long endTime = System.nanoTime();
				long duration = (endTime - startTime);
				System.out.println("Time: " + duration / 1000000L);
			}
			CSconn.close();
			FTPconn.close();
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static int countRow(ResultSet rs) throws SQLException {
		int rowcount = 0;
		if (rs.last()) {
			rowcount = rs.getRow();
			rs.beforeFirst();
		}
		return rowcount;
	}

	private static void export(ResultSet FTPrs) throws SQLException, IOException {
		Date date = new Date();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
		String filename = ("G:\\Data\\shipping weight\\AW" + ".csv");
		// + dateFormat.format(date) +".csv");
		FileWriter fw = new FileWriter(filename);
		fw.append("Itemno1, Sweight \n");
		int row = 1; // count total rows
		while (FTPrs.next()) {
			// row = row+1;
			// System.out.println(FTPrs.getString(1) +"," + FTPrs.getString(2));
			fw.append(FTPrs.getString(1) + "," + FTPrs.getString(2));
			fw.append(',' + "\n");
			row++;
		}
		// pop out msg
		System.out.println("CSV Exported\nsingle item rows: " + row);
		Component frame = null;
		JOptionPane.showMessageDialog(frame, "CSV Exported\nsingle item rows: " + row, "Message",
				JOptionPane.PLAIN_MESSAGE);
		fw.flush();
		fw.close();
	}

	public void printResult(ResultSet rs) throws SQLException {
		// int count=0;
		while (rs.next()) {
			int i = 1;
			System.out.println(rs.getString(i) + "," + rs.getString(i + 1));
			i++;
			// count++;
		}
		// return count;
	}
}
