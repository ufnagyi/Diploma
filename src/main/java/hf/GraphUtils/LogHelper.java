package hf.GraphUtils;


import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public enum LogHelper {
    INSTANCE;
    private static final DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    private static final SimpleDateFormat timerFormat = new SimpleDateFormat("mm:ss");
    private PrintWriter log;
    private Calendar startTime;

    LogHelper() {
        try {
            log = new PrintWriter(new FileWriter("results.log",true));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void logToFileT(String s){
        StringBuilder sb = new StringBuilder(s);
        for(int i = 0; i < Math.ceil((100 - s.length()) / 4); i++)
            sb.append("\t");
        sb.append(dateFormat.format(Calendar.getInstance().getTimeInMillis()));
        log.println(sb.toString());
        log.flush();
        System.out.println(s);
    }
    public void logToFileStartTimer(String s){
        StringBuilder sb = new StringBuilder(s);
        for(int i = 0; i < Math.ceil((100 - s.length()) / 4); i++)
            sb.append("\t");
        startTime = Calendar.getInstance();
        sb.append(dateFormat.format(startTime.getTimeInMillis()));
        log.println(sb.toString());
        log.flush();
        System.out.println(s);
    }
    public void logToFileStopTimer(String s){
        StringBuilder sb = new StringBuilder(s);
        for(int i = 0; i < Math.ceil((100 - s.length()) / 4); i++)
            sb.append("\t");
        sb.append(timerFormat.format(Calendar.getInstance().getTimeInMillis() - startTime.getTimeInMillis()));
        log.println(sb.toString());
        log.flush();
        System.out.println(s);
    }
    public void logToFile(String s) {
        log.println(s);
        log.flush();
        System.out.println(s);
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
        Runtime runtime = Runtime.getRuntime();
        long usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / MB;
        String usedMemo = "Felhasznált memória: " + usedMemory + " MB";
        log.println(usedMemo);
        log.flush();
        System.out.println(usedMemo);
    }
    public void close(){
        log.close();
    }
}