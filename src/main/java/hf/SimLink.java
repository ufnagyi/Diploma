package hf;


import org.neo4j.graphdb.Node;

/**
 * 2 node közti similarity kapcsolat dobozolása
 * Fontos: NEM tud irányt megkülönböztetni!
 */
public class SimLink<N extends Comparable<N>> extends Link{
    public double similarity;

    public SimLink(N l, N l1, double sim) {
        super(l, l1);
        similarity = sim;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SimLink simLink = (SimLink) o;

        if (startNode != simLink.startNode) return false;
        return endNode == simLink.endNode;

    }

    public String print(){
        return (startNode + ";" + endNode + ";" + similarity);
    }

}
