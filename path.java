import java.nio.file.Path;
import java.nio.file.Paths;

public class path{
    public static void main(String[] args){
        Path listing = Paths.get("./Hello.java");
        System.out.println(listing.getFileName());
        System.out.println(listing.getNameCount());
    }
}
