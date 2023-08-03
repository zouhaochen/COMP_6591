package Table;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * the panda like dataframe, use for control annotation and datbase
 */
public class Table {

    /**
     * table name
     */
    public String d_tableName;
    /**
     * column name
     */
    public ArrayList<String> d_title = new ArrayList<>();
    /**
     * 2D matrix for store data
     */
    public ArrayList<ArrayList<String>> d_contentTable = new ArrayList<>();
    /**
     * column counter
     */
    public int d_columnCounter;


    /**
     * table constructor
     * @param name need table name
     */
    public Table(String name) {
        this.d_tableName = name;
    }

    /**
     * create new column in table by input column name
     * @param p_newColumnName
     */
    public void createColumn(String p_newColumnName) {
        String[] columnName = p_newColumnName.split(",");
        this.d_columnCounter = columnName.length;
        for (int i = 0; i < d_columnCounter; i++) {
            columnName[i] = columnName[i].replaceAll("\\s", "");
            this.d_title.add(columnName[i]);
        }
    }

    /**
     * by input string to add new row in table
     * @param p_newRowStr
     */
    public void addRow(String p_newRowStr) {
        String[] records = p_newRowStr.split(",");
        ArrayList<String> row = new ArrayList<>();
        for (int i = 0; i < d_columnCounter; i++) {
            records[i] = records[i].replaceAll("\\s", "");
            row.add(records[i]);
        }
        d_contentTable.add(row);
    }


    /**
     * class toString method override
     * @return table string output
     */
    @Override
    public String toString(){
        //title
        System.out.println("---------------" + d_tableName.toUpperCase() + "---------------");
        List<List<String>> rows = new ArrayList<>();
        rows.add(d_title);
        for(ArrayList<String> row : d_contentTable){
            rows.add(row);
        }
        return formatAsTable(rows);
    }


    /**
     * table output format function
     * @param p_rawTable
     * @return
     */
    public static String formatAsTable(List<List<String>> p_rawTable)
    {
        int[] maxLengths = new int[p_rawTable.get(0).size()];
        for (List<String> row : p_rawTable)
        {
            for (int i = 0; i < row.size(); i++)
            {
                maxLengths[i] = Math.max(maxLengths[i], row.get(i).length());
            }
        }

        StringBuilder formatBuilder = new StringBuilder();
        for (int maxLength : maxLengths)
        {
            formatBuilder.append("%-").append(maxLength + 2).append("s");
        }
        String format = formatBuilder.toString();

        StringBuilder l_formattedTable = new StringBuilder();
        for (List<String> row : p_rawTable)
        {
            l_formattedTable.append(String.format(format, row.toArray(new String[0]))).append("\n");
        }
        return l_formattedTable.toString();
    }

}
