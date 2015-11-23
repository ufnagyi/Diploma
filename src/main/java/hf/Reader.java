package hf;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import onlab.core.Database;
import onlab.core.ExtendedDatabase;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

public class Reader {

    private Database db;
    private ExtendedDatabase dbExt;
    private final static char defaultSeparator = ';';
    private final static DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    private TObjectIntHashMap actorIDs;
    private TObjectIntHashMap directorIDs;
    private TObjectIntHashMap vodIDs;

    public Reader(Database db_) {
        this.db = db_;
        dbExt = (ExtendedDatabase) this.db;
    }

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


    public void createAllCSVs(){
        //node CSV letrehozas:
        this.createNewActorListCSV();
        this.createNewDirectorListCSV();
        this.createNewItemListCSV();
        this.createNewUserListCSV();
        this.createNewVODMenuListCSV();
        //relacio CSV letrehozas:
        this.createNewACTSINRelListCSV();
        this.createNewDIRBYRelListCSV();
        this.createNewSEENRelListCSV();
        this.createNewHASMETARelListCSV();
    }

    public void createNewItemListCSV() {
        String outputName = "items_for_graphDB.csv";
        System.out.println("Item list CSV keszitese: " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
        try {
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputName));
            bufferedWriter.write("ItemID:ID(Item)" + defaultSeparator + "title");
            bufferedWriter.newLine();
            for (Database.Item i : dbExt.items(null)) {
                bufferedWriter.write(Integer.toString(dbExt.getItemId(i.idx)) + defaultSeparator + "\"" + i.name.replace("\"","\\\"") + "\"");
                bufferedWriter.newLine();
            }
            bufferedWriter.close();
        } catch (IOException e) {
            System.err.println("Caught IOException:" + e.getMessage());
            return;
        }
        System.out.println("Item list CSV keszitese KESZ: " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
    }

    public void createNewActorListCSV() {
        String outputName = "actors_for_graphDB.csv";
        System.out.println("Actor list CSV keszitese: " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));

        Set<String> actors = new HashSet<>();
        for (Database.Item i : dbExt.items(null)) {
            for (String actor : dbExt.getItemKeyValue(i.idx, "Actor").split("\f")) {
                if (!actor.equals("") && !actor.equals("NA") && !actor.equals("na") && !actor.equals("n/a") && !actor.equals("N\\A") && !actor.equals("N/A"))
                    actors.add(actor);
            }
        }

        actorIDs = new TObjectIntHashMap();
        int j = 0;
        for (String actor : actors) {
            actorIDs.put(actor, j);
            j++;
        }

        int res = writeAllToCSV(actorIDs, "ActorID:ID(Actor)" + defaultSeparator + "name", outputName);
        if (res == 1)
            System.out.println("Actor list CSV keszitese KESZ: " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
    }

    public void createNewDirectorListCSV() {
        String outputName = "directors_for_graphDB.csv";
        System.out.println("Director list CSV keszitese: " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));

        Set<String> directors = new HashSet<>();
        for (Database.Item i : dbExt.items(null)) {
            for (String director : dbExt.getItemKeyValue(i.idx, "Director").split("\f")) {
                if (!director.equals("") && !director.equals("NA") && !director.equals("na") && !director.equals("n/a") && !director.equals("N\\A") && !director.equals("N/A") && !director.equals("n/d"))
                    directors.add(director);
            }
        }

        directorIDs = new TObjectIntHashMap();
        int j = 0;
        for (String director : directors) {
            directorIDs.put(director, j);
            j++;
        }

        int res = writeAllToCSV(directorIDs, "DirID:ID(Director)" + defaultSeparator + "name", outputName);
        if (res == 1)
            System.out.println("Director list CSV keszitese KESZ: " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
    }

    public void createNewVODMenuListCSV() {
        String outputName = "vodmenu_for_graphDB.csv";
        System.out.println("Vodmenu list CSV keszitese: " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));

        Set<String> vodMenuWords = new HashSet<>();
        for (Database.Item i : dbExt.items(null)) {
            HashSet<String> mWords = getUniqueItemMetaWordsByKey(i.idx, "VodMenuDirect", "[\f/]");
            for (String mW : mWords)
                vodMenuWords.add(mW);
        }

        vodIDs = new TObjectIntHashMap();
        int j = 0;
        for (String vod : vodMenuWords) {
            vodIDs.put(vod, j);
            j++;
        }

        int res = writeAllToCSV(vodIDs, "VodID:ID(VOD)" + defaultSeparator + "word", outputName);
        if (res == 1)
            System.out.println("Vodmenu list CSV keszitese KESZ: " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
    }

    public void createNewUserListCSV() {
        String outputName = "users_for_graphDB.csv";
        System.out.println("User list CSV keszitese: " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
        try {
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputName));
            bufferedWriter.write("UserID:ID(User)");
            bufferedWriter.newLine();
            for (Database.User u : dbExt.users(null)) {
                bufferedWriter.write(Integer.toString(dbExt.getUserId(u.idx)));
                bufferedWriter.newLine();
            }
            bufferedWriter.close();
        } catch (IOException e) {
            System.err.println("Caught IOException:" + e.getMessage());
            return;
        }
        System.out.println("User list CSV keszitese KESZ: " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
    }

    public void createNewSEENRelListCSV() {
        String outputName = "events_for_graphDB.csv";
        System.out.println("Event list CSV keszitese: " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));

        TIntObjectHashMap<HashSet<Integer>> uniqueEvents = new TIntObjectHashMap<>();
        for (Database.Event e : dbExt.events(null)) {
            if (!uniqueEvents.containsKey(dbExt.getUserId(e.uIdx))) {
                HashSet<Integer> hs = new HashSet<>();
                hs.add(dbExt.getItemId(e.iIdx));
                uniqueEvents.put(dbExt.getUserId(e.uIdx), hs);
            } else {
                uniqueEvents.get(dbExt.getUserId(e.uIdx)).add(dbExt.getItemId(e.iIdx));
            }
        }
        try {
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputName));
            bufferedWriter.write(":START_ID(User)" + defaultSeparator + ":END_ID(Item)");
            bufferedWriter.newLine();
            TIntObjectIterator<HashSet<Integer>> iterator = uniqueEvents.iterator();
            while(iterator.hasNext()){
                iterator.advance();
                String uIdx = Integer.toString(iterator.key());
                for(int iIdx : iterator.value()) {
                    bufferedWriter.write(uIdx + defaultSeparator + Integer.toString(iIdx));
                    bufferedWriter.newLine();
                }
            }
            bufferedWriter.close();
        } catch (IOException e) {
            System.err.println("Caught IOException:" + e.getMessage());
        }
        System.out.println("Event list CSV keszitese KESZ: " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
    }

    public void createNewACTSINRelListCSV() {
        String outputName = "acts_in_for_graphDB.csv";
        System.out.println("Acts_in list CSV keszitese: " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
        try {
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputName));
            bufferedWriter.write(":START_ID(Actor)" + defaultSeparator + ":END_ID(Item)");
            bufferedWriter.newLine();
            for (Database.Item i : dbExt.items(null)) {
                for (String actor : dbExt.getItemKeyValue(i.idx, "Actor").split("\f")) {
                    if (!actor.equals("") && !actor.equals("NA") && !actor.equals("na") && !actor.equals("n/a") && !actor.equals("N\\A") && !actor.equals("N/A")) {
                        int actorID = actorIDs.get(actor);
                        bufferedWriter.write(Integer.toString(actorID) + defaultSeparator + Integer.toString(dbExt.getItemId(i.idx)));
                        bufferedWriter.newLine();
                    }
                }
            }
            bufferedWriter.close();
        } catch (IOException e) {
            System.err.println("Caught IOException:" + e.getMessage());
        }
        System.out.println("Acts_in list CSV keszitese KESZ: " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
    }

    public void createNewDIRBYRelListCSV() {
        String outputName = "dir_by_for_graphDB.csv";
        System.out.println("Dir_by list CSV keszitese: " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
        try {
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputName));
            bufferedWriter.write(":START_ID(Item)" + defaultSeparator + ":END_ID(Director)");
            bufferedWriter.newLine();
            for (Database.Item i : dbExt.items(null)) {
                for (String director : dbExt.getItemKeyValue(i.idx, "Director").split("\f")) {
                    if (!director.equals("") && !director.equals("NA") && !director.equals("na") && !director.equals("n/a") && !director.equals("N\\A") && !director.equals("N/A") && !director.equals("n/d")) {
                        int directorID = directorIDs.get(director);
                        bufferedWriter.write(Integer.toString(dbExt.getItemId(i.idx)) + defaultSeparator + Integer.toString(directorID));
                        bufferedWriter.newLine();
                    }
                }
            }
            bufferedWriter.close();
        } catch (IOException e) {
            System.err.println("Caught IOException:" + e.getMessage());
        }
        System.out.println("Dir_by list CSV keszitese KESZ: " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
    }

    public void createNewHASMETARelListCSV() {
        String outputName = "has_meta_for_graphDB.csv";
        System.out.println("Has_meta list CSV keszitese: " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
        try {
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputName));
            bufferedWriter.write(":START_ID(Item)" + defaultSeparator + ":END_ID(VOD)");
            bufferedWriter.newLine();
            for (Database.Item i : dbExt.items(null)) {
                HashSet<String> mWords = getUniqueItemMetaWordsByKey(i.idx, "VodMenuDirect", "[\f/]");
                for (String mW : mWords) {
                    int mWID = vodIDs.get(mW);
                    bufferedWriter.write(Integer.toString(dbExt.getItemId(i.idx)) + defaultSeparator + Integer.toString(mWID));
                    bufferedWriter.newLine();
                }
            }
            bufferedWriter.close();
        } catch (IOException e) {
            System.err.println("Caught IOException:" + e.getMessage());
        }
        System.out.println("Has_meta list CSV keszitese KESZ: " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
    }

    public int writeAllToCSV(TObjectIntHashMap<String> info, String header, String outputName) {
        try {
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputName));
            bufferedWriter.write(header);
            bufferedWriter.newLine();
            TObjectIntIterator<String> iterator = info.iterator();
            while (iterator.hasNext()) {
                iterator.advance();
                bufferedWriter.write(Integer.toString(iterator.value()) + defaultSeparator + "\"" + iterator.key().replace("\"","\\\"") + "\"");
                bufferedWriter.newLine();
            }
            bufferedWriter.close();
        } catch (IOException e) {
            System.err.println("Caught IOException:" + e.getMessage());
            return 0;
        }
        return 1;
    }

    public HashSet<String> getUniqueItemMetaWordsByKey(int iIdx, String key, String key_value_separator) {
        HashSet<String> itemMetaWords = new HashSet<>();
        String keyAll = dbExt.getItemKeyValue(iIdx, key);
        String[] values = keyAll.split(key_value_separator);

        for (String val : values) {
            if (!val.equals("") && val.length() > 2)
                itemMetaWords.add(val);
        }
        return itemMetaWords;
    }
}
	
	
