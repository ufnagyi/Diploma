package hf.GraphUtils;

public class Link<N extends Comparable<N>> {
    public N startNode;
    public N endNode;

    public Link(){}

    public Link(N l, N l1){
        if (l == l1) {
            System.out.println("Egyforma indexek: " + l);
            throw new ExceptionInInitializerError();
        }
        if (l.compareTo(l1) < 1) {
            this.startNode = l;
            this.endNode = l1;
        } else {
            this.startNode = l1;
            this.endNode = l;
        }
        startNode = l;
        endNode = l1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Link link = (Link) o;

        if (startNode != link.startNode) return false;
        return endNode == link.endNode;

    }

    @Override
    public int hashCode() {
        return (startNode + "; " + endNode).hashCode();
    }
}
