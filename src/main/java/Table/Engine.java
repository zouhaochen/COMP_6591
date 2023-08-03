package Table;

import DataBase.DataBase;
import Table.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Stack;

/**
 * the annotation based engine
 */
public class Engine {

    /**
     * p_operationType 1 : bag 2 : probability  3 : certainty  4 : polynomial 5 : sql
     */
    public int d_type;
    /**
     * the database
     */
    public DataBase d_dataBase;


    public Engine(int p_type, DataBase p_dataBase) {
        this.d_type = p_type;
        this.d_dataBase = p_dataBase;
    }

    /**
     * execute query from input
     *
     * project -> @
     * select -> #
     * join -> *
     * union -> +
     * @param p_queryFormula
     * @return
     */
    public Table executeQuery(String p_queryFormula) throws Exception {
        Stack<String> operation = new Stack<>();
        Stack<Table> result = new Stack<>();

        p_queryFormula = formate(p_queryFormula);

        for (int i = 0; i < p_queryFormula.length(); i++) {
            String op = p_queryFormula.substring(i, i + 1);

            if (op.equals("(")) {
                operation.push(op);

            } else if (op.equals("@")) {
                String p = getProjectOrSelect(i, p_queryFormula);
                i = i + p.length() - 1;
                operation.push(p);

            } else if (op.equals("#")) {
                String s = getProjectOrSelect(i, p_queryFormula);
                i = i + s.length() - 1;
                operation.push(s);

            } else if (op.equals("*")) {
                operation.push("*");

            } else if (op.equals("+")) {
                operation.push("+");

            } else if (op.equals(")")) {
                String operator1 = operation.pop();
                String operator2 = operation.pop();
                Table res;

                if (operator1.equals("(")) {
                    if (operator2.contains("@") || operator2.contains("#")) {
                        Table t1 = result.pop();
                        res = executeUnaryOp(operator2, t1);
                    } else {
                        Table t1 = result.pop();
                        Table t2 = result.pop();
                        res = executeBiOp(operator2, t1, t2);
                    }
                } else {
                    if (operator1.contains("@") || operator2.contains("#")) {
                        Table t1 = result.pop();
                        res = executeUnaryOp(operator1, t1);
                    } else {
                        Table t1 = result.pop();
                        Table t2 = result.pop();
                        res = executeBiOp(operator1, t1, t2);
                    }
                }
                result.push(res);

            } else if (op.equals(" ")) {
                continue;
            } else {
                String tableName = getTableName(i, p_queryFormula);
                i = i + tableName.length() - 1;
                Table table = d_dataBase.database.get(tableName);
                result.push(table);
            }

        }
        if (operation.size() != 0) {
            String op = operation.pop();
            if (op.contains("@") || op.contains("#")) {
                Table table = result.pop();
                result.push(executeUnaryOp(op, table));
            } else {
                Table table1 = result.pop();
                Table table2 = result.pop();
                result.push(executeBiOp(op, table1, table2));
            }
        }


        return result.pop();

    }

    /**
     * used to execute single variable predicate
     *
     * @param p_operator
     * @param p_table    only one table needed
     * @return
     * @throws Exception
     */
    private Table executeUnaryOp(String p_operator, Table p_table) throws Exception {
        Table res = new Table("");
        if (p_operator.contains("@")) {
            String columns = p_operator.replaceAll("@", "")
                    .replaceAll("<", "")
                    .replaceAll(">", "");
            res = projectForAll(columns, p_table, Integer.toString(this.d_type));
        } else if (p_operator.contains("#")) {
            String conditions = p_operator.replaceAll("#", "");
            conditions = conditions.substring(1, conditions.length() - 1);

            res = selectForAll(conditions, p_table);
        }
        return res;
    }

    /**
     * used to execute double variables predicate
     *
     * @param p_operator
     * @param p_tableA   Table A
     * @param p_tableB   Table B
     * @return
     * @throws Exception
     */
    private Table executeBiOp(String p_operator, Table p_tableA, Table p_tableB) throws Exception {
        if (p_operator.contains("*")) {
            return joinForAll(p_tableA, p_tableB, Integer.toString(this.d_type));
        } else if (p_operator.contains("+")) {
            return unionForAll(p_tableA, p_tableB, Integer.toString(this.d_type));
        } else {
            return new Table("");
        }
    }

    /**
     * get rid of operator but return the table name from string of formula
     *
     * @param start   from where in strint to start find the table name
     * @param formula
     * @return
     */
    private String getTableName(int start, String formula) {
        int end = -1;
        for (int i = start; i < formula.length(); i++) {
            if (formula.charAt(i) == ')') {
                end = i;
                break;
            }
        }
        return formula.substring(start, end);
    }

    /**
     * get select string from formula
     *
     * @param start
     * @param formula
     * @return
     */
    private String getProjectOrSelect(int start, String formula) {
        int end = -1;
        for (int i = start + 1; i < formula.length(); i++) {
            if (formula.charAt(i) == '>' && formula.charAt(i + 1) == '(') {
                end = i;
                break;
            }
        }
        return formula.substring(start, end + 1);
    }

    /**
     * formula type form name to sign
     *
     * @param formula
     * @return sign
     */
    private String formate(String formula) {
        return formula.replaceAll("project", "@")
                .replaceAll("select", "#")
                .replaceAll("join", "*")
                .replaceAll("union", "+");
    }



    /*
    use =,!,<,>  the first is title after is the value, use "," to split each condition and use space" "to split title operator and value
    such as name = ABC,ID < 15
     */

    /**
     * select operation for table
     *
     * @param p_conditions SQL condition
     * @param p_table selected table
     * @return
     */
    public Table selectForAll(String p_conditions, Table p_table) {
        String[] separateConditions = p_conditions.split(",");

        Table l_selectTable = new Table("Select Table");
        l_selectTable.d_columnCounter = p_table.d_columnCounter;
        l_selectTable.d_title = p_table.d_title;

        for (ArrayList<String> eachLineInTable : p_table.d_contentTable) {
            boolean satisfyCondition = true;

            for (int i = 0; i < separateConditions.length; i++) {
                String[] presentCondition = separateConditions[i].split(" ");
                String title = presentCondition[0];
                String operator = presentCondition[1];
                String value = presentCondition[2];

                int presentTitleLocation = p_table.d_title.indexOf(title);

                if(operator.equals("=")){
                    if (!eachLineInTable.get(presentTitleLocation).equals(value)) {
                        satisfyCondition = false;
                    }
                }else if(operator.equals("!")){
                        if (eachLineInTable.get(presentTitleLocation).equals(value)) {
                            satisfyCondition = false;
                        }
                }else if (operator.equals(">")){
                    if (Integer.parseInt(eachLineInTable.get(presentTitleLocation)) <= Integer.parseInt(value)) {
                        satisfyCondition = false;
                    }
                }else if(operator.equals("<")){
                    if (Integer.parseInt(eachLineInTable.get(presentTitleLocation)) >= Integer.parseInt(value)) {
                        satisfyCondition = false;
                    }
                }
                if (!satisfyCondition) {
                    break;
                }
            }
            if (satisfyCondition) {
                l_selectTable.d_contentTable.add(eachLineInTable);
            }
        }
        return l_selectTable;
    }



    /**
     * project operation
     *
     * @param p_columns
     * @param p_table
     * @param p_operationType 1 : bag 2 : probability  3 : certainty  4 : polynomial 5 : normal
     * @return
     * @throws Exception
     */
    public Table projectForAll(String p_columns, Table p_table, String p_operationType) throws Exception {

        Table l_projectTable = new Table("Project Table");
        String[] l_columnArr = p_columns.split(",");
        String l_columnsAndannotation = p_columns + ",a nnotation";
        l_projectTable.createColumn(l_columnsAndannotation);

        for (String column : l_projectTable.d_title) {
            if (!p_table.d_title.contains(column)) {
                System.out.println("ERROR: wrong project column");
            }
        }
        ArrayList<ArrayList<String>> newContent = new ArrayList<ArrayList<String>>();
        for (ArrayList<String> row : p_table.d_contentTable) {
            ArrayList<String> newRow = new ArrayList<String>();
            for (String column : l_projectTable.d_title) {
                newRow.add(row.get(p_table.d_title.indexOf(column)));
            }
            boolean findNewAnnotation = false;
            for (ArrayList<String> lineInNewContent : newContent) {
                boolean findSameContentRow = true;

                // termination condition when alpha i is equal to alpha i+1
                for (String s : l_columnArr) {
                    if (!newRow.get(l_projectTable.d_title.indexOf(s)).equals(lineInNewContent.get(l_projectTable.d_title.indexOf(s)))) {
                        findSameContentRow = false;
                        break;
                    }
                }
                if (findSameContentRow) {
                    String newAnnotation = "";
                    if(p_operationType.equals("1")) {
                        newAnnotation = Integer.parseInt(lineInNewContent.get(l_projectTable.d_title.size() - 1)) +
                                Integer.parseInt(newRow.get(l_projectTable.d_title.size() - 1)) +
                                "";
                    }else if(p_operationType.equals("2")){
                            newAnnotation = Float.parseFloat(lineInNewContent.get(l_projectTable.d_title.size() - 1)) +
                                    Float.parseFloat(newRow.get(l_projectTable.d_title.size() - 1)) -
                                    Float.parseFloat(lineInNewContent.get(l_projectTable.d_title.size() - 1)) *
                                            Float.parseFloat(newRow.get(l_projectTable.d_title.size() - 1)) +
                                    "";
                    }else if(p_operationType.equals("3")){
                            newAnnotation = Math.max(Float.parseFloat(lineInNewContent.get(l_projectTable.d_title.size() - 1)),
                                    Float.parseFloat(newRow.get(l_projectTable.d_title.size() - 1))) +
                                    "";
                    }else if(p_operationType.equals("4")){
                            newAnnotation = "(" +
                                    lineInNewContent.get(l_projectTable.d_title.size() - 1) +
                                    "+" +
                                    newRow.get(l_projectTable.d_title.size() - 1) +
                                    ")";
                    }else if(p_operationType.equals("5")) {
                        newAnnotation = Math.max(Integer.parseInt(lineInNewContent.get(l_projectTable.d_title.size() - 1)),
                                Integer.parseInt(newRow.get(l_projectTable.d_title.size() - 1))) +
                                "";
                    }

                    lineInNewContent.remove(l_projectTable.d_title.size() - 1);
                    lineInNewContent.add(newAnnotation);
                    findNewAnnotation = true;
                    break;
                }
            }
            if (!findNewAnnotation) {
                newContent.add(newRow);
            }

        }
        l_projectTable.d_contentTable.addAll(newContent);
        return l_projectTable;
    }


    /**
     * union two tables
     *
     * @param p_tableA
     * @param p_tableB
     * @param p_operationType 1 : bag 2 : probability  3 : certainty  4 : polynomial 5 : normal
     * @return
     * @throws Exception
     */
    public Table unionForAll(Table p_tableA, Table p_tableB, String p_operationType) throws Exception {

        if (p_tableA.d_title.size() != p_tableB.d_title.size()) {
            throw new Exception("ERROR: if two table union they must have same number of column");
        }
        // can't union same table
        for (String columnA : p_tableA.d_title) {
            if (!p_tableB.d_title.contains(columnA)) {
                throw new Exception("ERROR: wrong union, can't self-union ");
            }
        }

        Table l_unionTable = new Table("UnionTable");
        ArrayList<ArrayList<String>> newTableOfTableAB = new ArrayList<ArrayList<String>>();
        newTableOfTableAB.addAll(p_tableA.d_contentTable);

        HashMap<Integer, Integer> orderOfAMapToOrderOfB = new HashMap<Integer, Integer>();

        for (int i = 0; i < p_tableA.d_title.size(); i++) {
            orderOfAMapToOrderOfB.put(i, p_tableB.d_title.indexOf(p_tableA.d_title.get(i)));
        }
        for (ArrayList<String> lineInTableB :
                p_tableB.d_contentTable) {
            ArrayList<String> newLine = new ArrayList<>();
            for (int i = 0; i < p_tableB.d_title.size(); i++) {
                newLine.add(lineInTableB.get(orderOfAMapToOrderOfB.get(i)));
            }
            boolean findNewAnnotation = false;
            for (ArrayList<String> lineInTableA :
                    p_tableA.d_contentTable) {
                boolean findSameRowInA = true;

                for (int i = 0; i < p_tableA.d_title.size() - 1; i++) {//one row each column
                    // termination condition
                    if (!lineInTableA.get(i).equals(newLine.get(i))) {
                        findSameRowInA = false;
                        break;
                    }
                }
                if (findSameRowInA) {
                    // calculate annotation
                    String newAnnotation = "";
                    if(p_operationType.equals("1")){
                            newAnnotation = Integer.parseInt(lineInTableA.get(p_tableA.d_title.size() - 1)) +
                                    Integer.parseInt(newLine.get(p_tableA.d_title.size() - 1)) + "";
                    }else if(p_operationType.equals("2")){
                            newAnnotation = Float.parseFloat(lineInTableA.get(p_tableA.d_title.size() - 1)) +
                                    Float.parseFloat(newLine.get(p_tableA.d_title.size() - 1)) -
                                    Float.parseFloat(lineInTableA.get(p_tableA.d_title.size() - 1)) *
                                            Float.parseFloat(newLine.get(p_tableA.d_title.size() - 1)) +
                                    "";
                    }else if(p_operationType.equals("3")){
                            newAnnotation = Math.max(Float.parseFloat(lineInTableA.get(p_tableA.d_title.size() - 1)), Float.parseFloat(newLine.get(p_tableA.d_title.size() - 1))) + "";
                    }else if(p_operationType.equals("4")){
                            newAnnotation = "(" +
                                    lineInTableA.get(p_tableA.d_title.size() - 1) +
                                    "+" +
                                    newLine.get(p_tableA.d_title.size() - 1) +
                                    ")";
                    }else if(p_operationType.equals("5")){
                            newAnnotation = Math.max(Integer.parseInt(lineInTableA.get(p_tableA.d_title.size() - 1)), Integer.parseInt(newLine.get(p_tableA.d_title.size() - 1))) + "";

                    }
                    newTableOfTableAB.get(p_tableA.d_contentTable.indexOf(lineInTableA)).remove(p_tableA.d_title.size() - 1);
                    newTableOfTableAB.get(p_tableA.d_contentTable.indexOf(lineInTableA)).add(newAnnotation);

                    findNewAnnotation = true;
                    break;
                }
            }
            if (!findNewAnnotation) {
                //add newline cannot find the duplicate row in tableA
                newTableOfTableAB.add(newLine);
            }
        }
        l_unionTable.d_columnCounter = p_tableA.d_columnCounter;
        l_unionTable.d_title.addAll(p_tableA.d_title);
        l_unionTable.d_contentTable.addAll(newTableOfTableAB);
        return l_unionTable;
    }

    /**
     * join two tables
     *
     * @param p_tableA
     * @param p_tableB
     * @param p_operationType 1 : bag 2 : probability  3 : certainty  4 : polynomial 5 : normal
     * @return jointable
     */
    public Table joinForAll(Table p_tableA, Table p_tableB, String p_operationType) {

        Table l_joinTable = new Table("joinTable");
        l_joinTable.d_title.addAll(p_tableA.d_title);

        HashMap<Integer, Integer> sameColumnLocationFromAToB = new HashMap<Integer, Integer>();
        ArrayList<String> title = p_tableA.d_title;
        for (int i = 0; i < title.size() - 1; i++) {
            String titleInA = title.get(i);
            ArrayList<String> strings = p_tableB.d_title;
            for (int i1 = 0; i1 < strings.size() - 1; i1++) {
                String titleInB = strings.get(i1);
                if (titleInA.equals(titleInB)) {
                    sameColumnLocationFromAToB.put(p_tableA.d_title.indexOf(titleInA), p_tableB.d_title.indexOf(titleInB));
                }
            }
        }

        ArrayList<Integer> locationColumnInBNotInA = new ArrayList<>();
        l_joinTable.d_title.remove(l_joinTable.d_title.size() - 1);
        for (int i = 0; i < p_tableB.d_title.size(); i++) {
            if (!p_tableA.d_title.contains(p_tableB.d_title.get(i))) {
                locationColumnInBNotInA.add(i);
                l_joinTable.d_title.add(p_tableB.d_title.get(i));
            }
        }
        l_joinTable.d_title.add("annotation");
        l_joinTable.d_columnCounter = p_tableA.d_columnCounter + locationColumnInBNotInA.size();

        for (ArrayList<String> lineInA : p_tableA.d_contentTable) {
            for (ArrayList<String> lineInB : p_tableB.d_contentTable) {
                boolean rightnessFlag = true;
                for (Integer columnLocationInA : sameColumnLocationFromAToB.keySet()) {
                    // termination condition
                    if (!lineInA.get(columnLocationInA).equals(lineInB.get(sameColumnLocationFromAToB.get(columnLocationInA)))) {
                        rightnessFlag = false;
                        break;
                    }
                }
                if (rightnessFlag) {
                    ArrayList<String> newLine = new ArrayList<>();
                    for (int i = 0; i < lineInA.size() - 1; i++) {
                        newLine.add(lineInA.get(i));
                    }
                    for (int location : locationColumnInBNotInA) {
                        newLine.add(lineInB.get(location));
                    }
                    String newAnnotation = "";
                    if(p_operationType.equals("1")){
                            newAnnotation = Integer.parseInt(lineInA.get(p_tableA.d_title.size() - 1)) *
                                    Integer.parseInt(lineInB.get(p_tableB.d_title.size() - 1)) +
                                    "";
                    }else if(p_operationType.equals("2")){
                            newAnnotation = Float.parseFloat(lineInA.get(p_tableA.d_title.size() - 1)) *
                                    Float.parseFloat(lineInB.get(p_tableB.d_title.size() - 1)) +
                                    "";
                    }else if(p_operationType.equals("3")){
                            newAnnotation = Math.min(Float.parseFloat(lineInA.get(p_tableA.d_title.size() - 1)),
                                    Float.parseFloat(lineInB.get(p_tableB.d_title.size() - 1))) +
                                    "";
                    }else if(p_operationType.equals("4")){
                            newAnnotation = lineInA.get(p_tableA.d_title.size() - 1) + "*" + lineInB.get(p_tableB.d_title.size() - 1);
                    }else if(p_operationType.equals("5")){
                            newAnnotation = Math.max(Integer.parseInt(lineInA.get(p_tableA.d_title.size() - 1)),
                                    Integer.parseInt(lineInB.get(p_tableB.d_title.size() - 1))) +
                                    "";

                    }
                    newLine.add(newAnnotation);
                    l_joinTable.d_contentTable.add(newLine);
                }
            }
        }
        return l_joinTable;
    }
}


