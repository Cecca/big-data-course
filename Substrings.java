import java.util.Iterator;

// The purpose of this class is to show how to translate the following two 
// nested loops, which accumulate substrings in a list, into an iterator that
// generates substrings on the fly.
//  
//  String theString = "....";
//  ArrayList<String> substrings = new ArrayList<String>();
//  for (int begin=0; begin < theString.size(); begin++ {
//    for (int end=begin+1; end < theString.size(); end++) {
//      substrings.add(theString.substring(begin, end);
//    }
//  }
public class Substrings {

  public static class SubstringsIterator implements Iterator<String> {
    // This is the raw data
    private String theString;

    // The following two variables simulate the indices of the two nested loops
    private int begin;
    private int end;

    public SubstringsIterator(String s) {
      theString = s;
      begin = 0;
      end = 1;
    }

    // This method basically implements the stopping condition 
    // of the outer loop
    @Override
    public boolean hasNext() {
      return begin < theString.length();
    }

    @Override
    public String next() {
      String substring = theString.substring(begin, end);

      // This check implements the stopping condition of the inner loop,
      // and when the inner loop ends starts the next outer iteration.
      if (end < theString.length()) {
        end += 1;
      } else {
        begin += 1;
        end = begin + 1;
      }

      return substring;
    }

  }

  public static void main(String args[]) {

    String s = args[0];
    SubstringsIterator it = new SubstringsIterator(s);
    while (it.hasNext()) {
      System.out.println(it.next());
    }

  }

}
