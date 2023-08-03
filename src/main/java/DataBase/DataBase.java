package DataBase;

import Table.Table;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

/**
 * the general database class use for annolog system
 */
public class DataBase {
    /**
     * resource directory path
     */
    public String filepath;
    /**
     * hash for each table in dictionary
     */
    public HashMap<String, Table> database = new HashMap<>();

    public DataBase(String filepath) {
        this.filepath = filepath;
        this.buildDB();
    }

    /**
     * build database from resource directory
     */
    private void buildDB() {
        System.out.println("Loading Data....");
        File file = new File(this.filepath);
        File[] files = file.listFiles();

        for (File databaseFile : files) {
            if (databaseFile.getName().endsWith(".txt")) {
                String[] path = databaseFile.getName().split("/");
                String name = path[path.length - 1].replace(".txt", "");

                // create table by name
                Table table = new Table(name);

                // read File By lines
                BufferedReader reader = null;
                try {
                    reader = new BufferedReader(new FileReader(databaseFile));
                    String tempString = null;
                    int line = 1;
                    while ((tempString = reader.readLine()) != null) {
                        if (line == 1)
                            table.createColumn(tempString);
                        else
                            table.addRow(tempString);
                        line++;
                    }
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (reader != null) {
                        try {
                            reader.close();
                        } catch (IOException e1) {
                        }
                    }
                }
                this.database.put(name, table);
            }
        }
    }
}
