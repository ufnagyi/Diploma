package hf;

import onlab.core.Database;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by ufnagyi
 */
public class GraphDBPredictor {
    public GraphDB graphDB;
    public Database db;

    private static final String dbFolder = "C:/Users/ufnagyi/Documents/TestDB";

    //      C:/Users/ufnagyi/Documents/Neo4J_Database
    //      C:/Users/ufnagyi/Documents/TestDB
//         F:/Dokumentumok/Neo4j

    public GraphDBPredictor(){
        graphDB = new GraphDB();
    }

    public void setParameters(Database db){
        this.db = db;
    }



    public void train(){

//        Reader r = new Reader(db);
//        r.createNewItemActorList();
//        r.createNewItemDirectorList();
//        r.createNewItemVODMenuList();
//        r.createNewItemList();
//        r.createNewEventList();
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        System.out.println("GraphDB epites kezdese:" + dateFormat.format(Calendar.getInstance().getTimeInMillis()));

        //grafdb szolgáltatás elindítása
        graphDB.initDB(db, new File(dbFolder));

        //ha fel kell építeni:
        //graphDB.buildDBFromImpressDB();
        System.out.println("GraphDB szamitas kezdese:" + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
        graphDB.asd();


        System.out.println("A grafDB felepult:" + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
    }



}
