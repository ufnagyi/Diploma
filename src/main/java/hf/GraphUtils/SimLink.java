package hf.GraphUtils;


import java.util.Comparator;

/**
 * 2 node közti similarity kapcsolat dobozolása
 * Fontos: NEM tud irányt megkülönböztetni!
 */
public class SimLink<N extends Comparable<N>> extends Link<N> {
    public double similarity;

    public SimLink(N l, N l1){
        super(l,l1);
        similarity = 0.0;
    }

    public SimLink(N l, N l1, double sim) {
        super(l, l1);
        similarity = sim;
    }

    public static Comparator<SimLink<Long>> getComparator() {
        return (o1, o2) -> ((Double) o1.similarity).compareTo((Double) o2.similarity);
    }

    public String print(){
        return (startNode + ";" + endNode + ";" + similarity);
    }

}
