package hf.JavaUtils;


public class ObjectSim implements Comparable<ObjectSim>{
    public int idx;
    public double sim;

    public ObjectSim(int i, double s){
        this.idx = i;
        this.sim = s;
    }

    @Override
    public int compareTo(ObjectSim o) {
        return -Double.compare(this.sim,o.sim);
    }
}
