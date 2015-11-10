package hf;


import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public enum LogHelper {
    INSTANCE;
    private static final DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    public void log(String s) {
        System.out.println(s + "\t" + dateFormat.format(Calendar.getInstance().getTimeInMillis()));
    }
}