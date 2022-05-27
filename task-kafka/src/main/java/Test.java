import java.time.LocalDate;
import java.util.Calendar;

public class Test {
    public static void main(String[] args) {
        LocalDate today = LocalDate.now();
        LocalDate yesterday = today.minusDays(1);

        System.out.println(today);
        System.out.println(yesterday);
    }
}
