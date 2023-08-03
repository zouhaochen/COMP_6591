import DataBase.DataBase;
import Table.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

/**
 * the driver
 */
public class App {
    public static void main(String[] args) {
        System.out.println("====== Welcome to Annotation Log System ======");
        DataBase dataBase = new DataBase("src/main/resources/db");
        List<String> l_typeList = Arrays.asList("bag","probability","certainty","polynomial","SQL");
        while (true) {
            Scanner s = new Scanner(System.in);
            System.out.println("\n----- Please Input your query ----- ");
            String query = s.nextLine();
            System.out.println("||annotation Type selection||");
            System.out.println("||1.bag                    ||");
            System.out.println("||2.probability            ||");
            System.out.println("||3.certainty              ||");
            System.out.println("||4.polynomial             ||");
            System.out.println("||5.SQL                    ||");

            String type = s.nextLine();
            if (query.equals("exit"))
                break;
            Engine l_annoEngine = new Engine(Integer.valueOf(type), dataBase);
            try {
                // execute user input query in engine
                Table res = l_annoEngine.executeQuery(query);
                // output the query result with table format
                System.out.print(res);
                // operation type output
                System.out.println("^^^^^^^^^^ "+l_typeList.get(Integer.valueOf(type)-1).toUpperCase()+" ^^^^^^^^^^");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
}
