package hf;

import onlab.core.Database;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by ufnagyi
 */
public class GraphDBPredictor {
    public GraphDB graphDB;
    public Database db;

    public GraphDBPredictor(){
        graphDB = new GraphDB();
    }

    public void setParameters(Database db){
        this.db = db;
    }

    public void train(){
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        System.out.println("GraphDB epites kezdese:" + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
        graphDB.buildDB(db);
        System.out.println("A grafDB felepult:" + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
    }
}
