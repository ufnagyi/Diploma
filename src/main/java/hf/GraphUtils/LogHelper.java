package hf.GraphUtils;


import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public enum LogHelper {
    INSTANCE;
    private static final DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    private PrintWriter log;

    LogHelper() {
        try {
            log = new PrintWriter(new FileWriter("results.log",true));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void logToFileT(String s){
        log.println(s + "         " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
        log.flush();
    }
    public void logToFile(String s) {
        log.println(s);
        log.flush();
    }
    public void logSeparatorToFile() {
        StringBuilder sb = new StringBuilder("");
        for (int i = 0; i < 100; i++)
            sb.append('-');
        log.println(sb.toString());
    }
    public void logToFile(int num){
        logToFile(Integer.toString(num));
    }
    public void logToFile(double num){
        logToFile(Double.toString(num));
    }
    public void logToFile(long num){
        logToFile(Long.toString(num));
    }
    public void log(String s) {
        System.out.println(s + "  " + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
    }
    public void printMemUsage(){
        long MB = 1024*1024;
        log.println("Felhasznált memória: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / MB + " MB");
        log.flush();
    }
    public void close(){
        log.close();
    }
}