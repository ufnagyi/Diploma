package hf;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class Reader {

	public void modifyBUY(){
        System.out.println("Start");
        String filename = "events_final.csv";
        char separator = '\t';
        String outputName = "events_final2.csv";
        char separator2 = '\t';
		try{
			CSVReader reader = new CSVReader(new FileReader(filename),
					separator);
			CSVWriter writer = new CSVWriter(new FileWriter(outputName), separator2,
					CSVWriter.NO_QUOTE_CHARACTER);

			String[] nextLine;
			String[] key;
			boolean row = false;
			while ((nextLine = reader.readNext()) != null) {
				key = nextLine;
				if(row){
					key[3] = "2";
				}
				if(!row){
					row = true;
				}
				writer.writeNext(key);
			}
			writer.close();
			reader.close();
		} catch (IOException e) {
			System.err.println("Caught IOException:" + e.getMessage());
		}
		System.out.println("Done!");
	}
		
	
	
	
	
	
	
	
//	
//	
//	public void Read(String filename, char separator, String outputName,
//			char separator2) {
//		try {
//			CSVReader reader = new CSVReader(new FileReader(filename),
//					separator);
//			CSVWriter writer = new CSVWriter(new FileWriter(outputName), ',',
//					CSVWriter.NO_QUOTE_CHARACTER);
//
//			String[] nextLine;
//			String[] key;
//			String sep = String.valueOf(separator2);
//			while ((nextLine = reader.readNext()) != null) {
//				
//				key = nextLine[1].split(sep);
//				if (key.length > 0) {
//					for (int j = 0; j < key.length; j++) {
//						if (key[j].length() > 0 && !key[j].matches("NA")) {
//							nextLine[1] = key[j];
//							writer.writeNext(nextLine);
//						}
//					}
//				}
//			}
//			writer.close();
//			reader.close();
//		} catch (IOException e) {
//			System.err.println("Caught IOException:" + e.getMessage());
//		}
//		System.out.println("Done!");
//	}
}
