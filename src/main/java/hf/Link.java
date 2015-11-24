package hf;

public class Link {
    public long startNode;
    public long endNode;

    public Link(){}

    public Link(long st, long en){
        startNode = st;
        endNode = en;
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
