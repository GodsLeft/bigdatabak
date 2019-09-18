import java.net.URL;
public class BootstrapTest{
    public static void main(String[] args){
        URL[] urls = sun.misc.Launcher.getBootstrapClassPath().getURLs();
        for(URL tmp:urls){
            System.out.println(tmp.toExternalForm());
        }
    }
}
