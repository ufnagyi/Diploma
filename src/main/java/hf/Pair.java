package hf;

/**
 * @author ufnagyi
 *
 */
public class Pair implements java.io.Serializable {

	public int iIdx1;
	public int iIdx2;

	public Pair(int idx1, int idx2) {
        if(idx1 == idx2) {
            System.out.println("Egyforma indexek");
            throw new ExceptionInInitializerError();
        }
        if (idx1 < idx2) {
			this.iIdx1 = idx1;
			this.iIdx2 = idx2;
		} else {
			this.iIdx1 = idx2;
			this.iIdx2 = idx1;
		}

	}


    @Override
    public int hashCode() {
        return (iIdx1 + "; " + iIdx2).hashCode();
    }


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Pair other = (Pair) obj;
		if (iIdx1 != other.iIdx1)
			return false;
		if (iIdx2 != other.iIdx2)
			return false;
		return true;
	}


	@Override
	public String toString() {
		return "MoviePair [iIdx1=" + iIdx1 + ", iIdx2=" + iIdx2 + "]";
	}
}
