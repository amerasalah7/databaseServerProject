import java.io.Serializable;
import java.util.ArrayList;

public class Dimension  implements Serializable {
    ArrayList<String []> arr;
    public Dimension(ArrayList<String []> arr){
        this.arr = arr;
    }
}
