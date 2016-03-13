import java.io.File;
import java.util.HashMap;
import java.util.Scanner;
import java.util.Set;

/**
 * Created by Michael on 3/12/2016.
 */
public class PowerCompTest {

  public static void main(String[] args) {
    HashMap<String, Double> map1 = new HashMap<>();
    HashMap<String, Double> map2 = new HashMap<>();

    Scanner scanner = null;
    try {
      scanner = new Scanner(new File("household_power_consumption.txt"));
    } catch (Exception e) {
      System.exit(0);
    }

    while (scanner.hasNextLine()) {
      String value = scanner.nextLine();

      if (!(value.toString().charAt(0) == 'D') && !(value.toString().contains("?"))) {
        String data[] = value.toString().split(";");

        String date = data[0];
        Double sub2 = Double.parseDouble(data[7]);
        Double sub3 = Double.parseDouble(data[8]);

        if (map1.containsKey(date)) {
          map1.put(date, map1.get(date) + sub2);
          map2.put(date, map2.get(date) + sub3);
        } else {
          map1.put(date, sub2);
          map2.put(date, sub3);
        }
      }
    }

    Set<String> keys = map1.keySet();
    int count1 = 0;
    int count2 = 0;
    int count3 = 0;
    int count4 = 0;
    for (String key : keys) {
      if (map1.get(key) < 500) {
        count1++;
      } else {
        count2++;
      }

      if (map2.get(key) < 6000) {
        count3++;
      } else {
        count4++;
      }
//      System.out.println(key + ": " + map1.get(key) + ";" + map2.get(key));
    }
    System.exit(0);


  }
}
