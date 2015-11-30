package hf.GraphUtils;

public class DirectedLink<N extends Comparable<N>> extends Link<N> {
    public DirectedLink(N l, N l1){
        this.startNode = l;
        this.endNode = l1;
    }
}
