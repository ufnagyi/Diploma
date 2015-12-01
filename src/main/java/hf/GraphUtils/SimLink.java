package hf.GraphUtils;


import com.google.common.base.Objects;

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

    public String print(){
        return (startNode + ";" + endNode + ";" + similarity);
    }

}
