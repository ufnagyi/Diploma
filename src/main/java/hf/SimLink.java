package hf;


import org.neo4j.graphdb.Node;

/**
 * 2 node közti similarity kapcsolat dobozolása
 * Fontos: NEM tud irányt megkülönböztetni!
 */
public class SimLink extends Link{
    public double similarity;

    public SimLink(long l, long l1, double sim) {
        if (l == l1) {
            System.out.println("Egyforma indexek: " + l);
            throw new ExceptionInInitializerError();
        }
        if (l < l1) {
            this.startNode = l;
            this.endNode = l1;
        } else {
            this.startNode = l1;
            this.endNode = l;
        }
        similarity = sim;

    }

    public SimLink(Node n, Node n1, double sim){
        long l = n.getId();
        long l1 = n1.getId();
        if(l == l1) {
            System.out.println("Egyforma indexek: " + l);
            throw new ExceptionInInitializerError();
        }
        if (l < l1) {
            this.startNode = l;
            this.endNode = l1;
        } else {
            this.startNode = l1;
            this.endNode = l;
        }
        similarity = sim;
    }

    public SimLink(long l, long l1) {
        if (l == l1) {
            System.out.println("Egyforma indexek: " + l);
            throw new ExceptionInInitializerError();
        }
        if (l < l1) {
            this.startNode = l;
            this.endNode = l1;
        } else {
            this.startNode = l1;
            this.endNode = l;
        }
    }

    public SimLink(Node n, Node n1){
        long l = n.getId();
        long l1 = n1.getId();
        if(l == l1) {
            System.out.println("Egyforma indexek: " + l);
            throw new ExceptionInInitializerError();
        }
        if (l < l1) {
            this.startNode = l;
            this.endNode = l1;
        } else {
            this.startNode = l1;
            this.endNode = l;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SimLink simLink = (SimLink) o;

        if (startNode != simLink.startNode) return false;
        return endNode == simLink.endNode;

    }


    @Override
    public int hashCode() {
        return (startNode + "; " + endNode).hashCode();
    }

    public String print(){
        return (startNode + ";" + endNode + ";" + similarity);
    }

}
