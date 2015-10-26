package hf;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import onlab.core.Database;
import onlab.core.ExtendedDatabase;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashSet;

public class Reader {

    private Database db;
    private ExtendedDatabase dbExt;

    public Reader(Database db_){this.db = db_;
    dbExt = (ExtendedDatabase) this.db;}

    public void modifyBUY() {
        System.out.println("Start");
        String filename = "events_final.csv";
        char separator = '\t';
        String outputName = "events_final2.csv";
        char separator2 = '\t';
        try {
            CSVReader reader = new CSVReader(new FileReader(filename),
                    separator);
            CSVWriter writer = new CSVWriter(new FileWriter(outputName), separator2,
                    CSVWriter.NO_QUOTE_CHARACTER);

            String[] nextLine;
            String[] key;
            boolean row = false;
            while ((nextLine = reader.readNext()) != null) {
                key = nextLine;
                if (row) {
                    key[3] = "2";
                }
                if (!row) {
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


    public void createNewEventList() {
        String outputName = "events_for_graphDB.csv";
        char separator = ';';
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        System.out.println("Event list CSV keszitese: " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
        try {
            CSVWriter writer = new CSVWriter(new FileWriter(outputName), separator,
                    CSVWriter.NO_QUOTE_CHARACTER);
            String[] line = {"UserID", "ItemID"};
            writer.writeNext(line);
            for (Database.Event e : dbExt.events(null)) {
                writer.writeNext(new String[]{Integer.toString(e.uIdx), Integer.toString(e.iIdx)});
            }
            writer.close();
        } catch (IOException e) {
            System.err.println("Caught IOException:" + e.getMessage());
        }
        System.out.println("Event list CSV keszitese KESZ: " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
    }

    public void createNewItemList() {
        String outputName = "items_for_graphDB.csv";
        char separator = ';';
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        System.out.println("Item list CSV keszitese: " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
        try {
            CSVWriter writer = new CSVWriter(new FileWriter(outputName), separator,
                    CSVWriter.DEFAULT_QUOTE_CHARACTER);
            String[] line = {"ItemID", "Actor", "Director", "Title", "VODMenuDirect"};
            writer.writeNext(line);
            for (Database.Item i : dbExt.items(null)) {
                writer.writeNext(new String[]{Integer.toString(i.idx), dbExt.getItemKeyValue(i.idx, "Actor"), dbExt.getItemKeyValue(i.idx, "Director"), i.name, dbExt.getItemKeyValue(i.idx, "VodMenuDirect")});
            }
            writer.close();
        } catch (IOException e) {
            System.err.println("Caught IOException:" + e.getMessage());
            return;
        }
        System.out.println("Item list CSV keszitese KESZ: " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
    }

    public void createNewItemActorList() {
        String outputName = "items_actors_for_graphDB.csv";
        char separator = ';';
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        System.out.println("Itemactor list CSV keszitese: " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
        try {
            CSVWriter writer = new CSVWriter(new FileWriter(outputName), separator,
                    CSVWriter.DEFAULT_QUOTE_CHARACTER);
            String[] line = {"ItemID", "Actor"};
            writer.writeNext(line);
            for (Database.Item i : dbExt.items(null)) {
                String id = Integer.toString(i.idx);
                for (String actor : dbExt.getItemKeyValue(i.idx, "Actor").split("\f")) {
                    if (actor.equals("") || actor.equals("NA") || actor.equals("na") || actor.equals("n/a") || actor.equals("N\\A") || actor.equals("N/A"))
                        continue;
                    writer.writeNext(new String[]{id, actor});
                }
            }
            writer.close();
        } catch (IOException e) {
            System.err.println("Caught IOException:" + e.getMessage());
            return;
        }
        System.out.println("Itemactor list CSV keszitese KESZ: " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
    }

    public void createNewItemDirectorList() {
        String outputName = "items_directors_for_graphDB.csv";
        char separator = ';';
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        System.out.println("Itemdirector list CSV keszitese: " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
        try {
            CSVWriter writer = new CSVWriter(new FileWriter(outputName), separator,
                    CSVWriter.DEFAULT_QUOTE_CHARACTER);
            String[] line = {"ItemID", "Director"};
            writer.writeNext(line);
            for (Database.Item i : dbExt.items(null)) {
                String id = Integer.toString(i.idx);
                for (String director : dbExt.getItemKeyValue(i.idx, "Director").split("\f")) {
                    if (!director.equals("") && !director.equals("NA") && !director.equals("na") && !director.equals("n/a") && !director.equals("N\\A") && !director.equals("N/A") && !director.equals("n/d"))
                        writer.writeNext(new String[]{id, director});
                }
            }
            writer.close();
        } catch (IOException e) {
            System.err.println("Caught IOException:" + e.getMessage());
            return;
        }
        System.out.println("Itemdirector list CSV keszitese KESZ: " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
    }

    public void createNewItemVODMenuList() {
        String outputName = "items_vodmenu_for_graphDB.csv";
        char separator = ';';
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        System.out.println("Item vodmenu list CSV keszitese: " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
        try {
            CSVWriter writer = new CSVWriter(new FileWriter(outputName), separator,
                    CSVWriter.DEFAULT_QUOTE_CHARACTER);
            String[] line = {"ItemID", "VODMenu"};
            writer.writeNext(line);
            for (Database.Item i : dbExt.items(null)) {
                for (String mW : getUniqueItemMetaWordsByKey(i.idx,"VodMenuDirect","[\f/]")) {
                    String id = Integer.toString(i.idx);
                    writer.writeNext(new String[]{id, mW});
                }
            }
            writer.close();
        } catch (IOException e) {
            System.err.println("Caught IOException:" + e.getMessage());
            return;
        }
        System.out.println("Item vodmenu list CSV keszitese KESZ: " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
    }


    public HashSet<String> getUniqueItemMetaWordsByKey(int iIdx, String key, String key_value_separator){
        HashSet<String> itemMetaWords = new HashSet<>();
        String keyAll = dbExt.getItemKeyValue(iIdx, key);
        String[] values = keyAll.split(key_value_separator);

        for (String val : values) {
            if (val.equals("") || val.length() < 3)
                continue;
            itemMetaWords.add(val.toLowerCase());
        }
        return itemMetaWords;
    }
}
	
	
