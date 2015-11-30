package hf.JavaUtils;

public class WordPair implements java.io.Serializable {
	public String word1;
	public String word2;
	
	public WordPair(String w1, String w2){
		if(w1.compareToIgnoreCase(w2) > 0){
			word1 = w2;
			word2 = w1;
		}
		else {
			word1 = w1;
			word2 = w2;
		}
	}

	@Override
	public int hashCode() {
		return (word1 + "; " + word2).hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		WordPair other = (WordPair) obj;
		if (word1 == null) {
			if (other.word1 != null)
				return false;
		} else if (!word1.equals(other.word1))
			return false;
		if (word2 == null) {
			if (other.word2 != null)
				return false;
		} else if (!word2.equals(other.word2))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "WordPair [word1=" + word1 + ", word2=" + word2 + "]";
	}	
}
