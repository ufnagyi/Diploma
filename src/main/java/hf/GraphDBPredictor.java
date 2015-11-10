package hf;

import onlab.core.Database;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by ufnagyi
 */
abstract public class GraphDBPredictor {
    public GraphDB graphDB;

    public void setParameters(GraphDB gDB){
        graphDB = gDB;
    }

     public void train() {
         if (!this.graphDB.isInited())
             graphDB.initDB();

     }



    //predictor teszteléséhez
    public abstract void test();
}
