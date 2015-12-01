package hf.GraphUtils;

import com.google.common.base.Objects;

public class Link<N extends Comparable<N>> {
    public N startNode;
    public N endNode;

    public Link(){}

    public Link(N l, N l1){
        if (l == l1) {
            System.out.println("Egyforma indexek: " + l);
            throw new ExceptionInInitializerError();
        }
        if (l.compareTo(l1) < 0) {
            this.startNode = l;
            this.endNode = l1;
        } else {
            this.startNode = l1;
            this.endNode = l;
        }
    }

    @Override
    public int hashCode() { return (startNode + "; " + endNode).hashCode();}
    //    public int hashCode() {  return Objects.hashCode(startNode, endNode);  }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final Link other = (Link) obj;
        return Objects.equal(this.startNode, other.startNode)
                && Objects.equal(this.endNode, other.endNode);
    }
}
