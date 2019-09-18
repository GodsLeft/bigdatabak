import java.util.Collection;
import java.util.*;
public class CompareTo{
    public static void main(String[] args){
        List<Integer> values = new ArrayList(11,2,43,14,5,76,6);
        Collections.sort(values, new Comparator<Integer>(){
            @Override
            public int compare(Integer one, Integer other){
                return one.compareTo(other);
            }
        });
    }
}
