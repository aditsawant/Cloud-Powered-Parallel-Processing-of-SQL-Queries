package Utils;

import Spark.SparkExecutor;
import org.json.JSONArray;
import org.json.JSONObject;

import java.lang.reflect.Array;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLQueries {
    /*
    *
    * Select * from table where x == y
    * Select * from t1 inner join t2 on t1.a = t2.a where x == y
    * Select count(country), country from customers where customers.age < 50 group by customers.country having count(country) == 3
    *
    * */
//
//    public void execQuery1(JSONObject queryJSON){
//        String[] columns = (String[]) queryJSON.get("columns");
//        JSONObject whereJSON = (JSONObject) queryJSON.get("where");
//        String operator = (String) queryJSON.get((String) whereJSON.get("operator"));
//        String val1 = (String) queryJSON.get((String) whereJSON.get("value1"));
//        String val2 = (String) queryJSON.get((String) whereJSON.get("value2"));
//        String table = (String) queryJSON.get("table");
//
//        if(!operator.equalsIgnoreCase("like") || !operator.equalsIgnoreCase("in")){
//            switch(operator){
//                case "=":
//
//            }
//        }
//        for()
//    }

//    public static Dataset<Row> select(JSONObject queryJSON, Dataset<Row> dataset){
//        System.out.println("Selecting Manually\n");
////        String[] columns  = queryJSON.get("columns").toString().split(",");
//        JSONArray temp = (JSONArray) queryJSON.get("columns");
//
//        if(temp.get(0).equals("movietitle")) System.out.println("\n\n\nMatches");
//        else System.out.println("\n\n\n\n\nDidn't");
//
//        ArrayList<String> columns = new ArrayList<>();
//        for(int i = 0; i < temp.length(); i++){
//            columns.add((String) temp.get(i));
//        }
//
//        for (String s : columns) {
//            System.out.print(s + ", ");
//        }
//        System.out.println();
//
//        for(String s: dataset.columns()){
//            if(!columns.contains(s)){
//                dataset = dataset.drop(s);
//            }
//        }
//
////        Dataset<Row> result = null;
////        for(int i = 0; i < temp.length(); i++){
////            result = dataset.withColumn((String) temp.get(i), dataset.col((String) temp.get(i)));
////        }
//
////        ArrayList<String> notToBeDropped = new ArrayList<>();
////        for(String s: dataset.columns()){
////            for(String t: columns){
////                if(s.equalsIgnoreCase(t)){
////                    notToBeDropped.add(t);
////                }
////            }
////        }
////        for(String s: dataset.columns()){
////            if(!notToBeDropped.contains(s)){
////                dataset = dataset.drop(s);
////            }
////        }
////        dataset = dataset.drop();
//
//        return dataset;
//    }

    public static void select(JSONObject queryJSON, Table dataset){
        JSONArray temp = (JSONArray) queryJSON.get("columns");
        System.out.println(dataset.getTableName());
        ArrayList<String> columns = new ArrayList<>();
        if(temp.get(0).toString().equalsIgnoreCase("*")){
            System.out.println("\n\n\n\n ABOUT TO RETURN");
            return;
        }
        for (Object curr : temp) {
            if ((curr.toString()).charAt(0) != '{') {
                System.out.println(curr);
                columns.add(curr.toString());
            }
//            else {
//                JSONObject cJSON = (JSONObject) curr;
//                columns.add((String) cJSON.get("column"));
//            }
        }
        Set<String> set = new LinkedHashSet<>(columns);
        columns = new ArrayList<>(set);
//        for(int i = 0; i < temp.length(); i++){
//            columns.add((String) temp.getJSONObject(i));
//        }
//        System.out.println(columns.toString());
        JSONObject tableJSON = (JSONObject) Table.getMappingJSON().get(dataset.getTableName());
//        String[] all_columns = tableJSON.keySet().toArray();
        System.out.println(tableJSON.toString());
        ArrayList<String> all_columns = new ArrayList<>();
//        for (Iterator<String> it = tableJSON.keys(); it.hasNext(); ) {
//            String key = it.next();
//            if(key != null) all_columns.add(key);
//            else System.out.println("BC kya chutiyap");
//        }
        System.out.println(columns.toString());
        System.out.println(all_columns.toString());

        ArrayList<String> columnNames = new ArrayList<>();
        if(queryJSON.opt("joinType") == null){
            all_columns = new ArrayList<>(Arrays.asList(SparkExecutor.headers.get((String) queryJSON.get("table"))));
            columnNames = new ArrayList<>(Arrays.asList(SparkExecutor.headers.get((String) queryJSON.get("table"))));
        } else {
            JSONObject newTableJSON = (JSONObject) queryJSON.get("table");
            String tableName = newTableJSON.get("table1") + "X" + newTableJSON.get("table2");
//            columnNames = new ArrayList<>(Arrays.asList(SparkExecutor.headers.get(newTableJSON)));
            JSONObject obj = (JSONObject) Table.getMappingJSON().get(tableName);
            for(int i = 0; i < obj.length(); i++){
                for (Iterator<String> it = obj.keys(); it.hasNext(); ) {
                    String key = it.next();
                    if((int) obj.get(key) == i) {
                        all_columns.add(key);
                        columnNames.add(key);
                    }
                }
            }
        }
        System.out.println(columns.toString());
        System.out.println(all_columns.toString());
        System.out.println("ColumnNames: " + columnNames.toString());


        // Case 1: [HANDLED] No aggregates anywhere.
        // Case 2: [FUCK THIS] Aggregates in select columns without a following group-by clause.
        // Case 3: Aggregates in select columns followed by a group-by clause.

        if(dataset.aggMap != null){
            // Case 3
            for(String s: all_columns){
                if(!columns.contains(s)){
                    System.out.println("About to drop " + s);
                    for(ArrayList<Object> row: dataset.table){
                        row.remove(columnNames.indexOf(s));
                    }
                    columnNames.remove(s);
                }
            }
            // add extra column
            ArrayList<Object> toRemove = new ArrayList<>();
            for(ArrayList<Object> row: dataset.table){
                if(dataset.aggMap.get(row.get(0)) != null)
                    row.add(dataset.aggMap.get(row.get(0)));
                else {
                    toRemove.add(row);
                }
            }
            for(Object obj: toRemove) {
                dataset.table.remove(obj);
            }
            ArrayList<ArrayList<Object>> newTable = new ArrayList<>();
            //ALTERNATIVE WAY by AMEY
//            if(queryJSON.get("having") != null){
//                JSONObject havingJSON = (JSONObject) queryJSON.get("having");
//
//                for(ArrayList<Object> row: dataset.table){
//                    if(!newTable.contains(row)){
//                        newTable.add(row);
//                    }
//                }
//            }
//            else{
            for(ArrayList<Object> row: dataset.table){
                if(!newTable.contains(row)){
                    newTable.add(row);
                }
            }
//            }
            dataset.setTable(newTable);
        } else {
            // Case 1
            for(String s: all_columns){
                if(!columns.contains(s)){
                    System.out.println("About to drop " + s);
                    for(ArrayList<Object> row: dataset.table){
                        row.remove(columnNames.indexOf(s));
                    }
                    columnNames.remove(s);
                }
            }
        }

//        System.out.println("This size: " + dataset.table.size());
//        return dataset;
    }

    public static void where(JSONObject queryJSON, Table dataset){
        System.out.println("Got inside where");
        JSONObject whereJSON = (JSONObject) queryJSON.get("where");
        String operator = (String) whereJSON.get("operator");
        String val1 = (String) whereJSON.get("value1");

        ArrayList<String> val2Array = new ArrayList<>();
        String val2 = null;

        if(operator.equalsIgnoreCase("IN")){
            val2Array = (ArrayList<String>) whereJSON.get("value2");
        } else {
            val2 = (String) whereJSON.get("value2");
        }
        Double epsilon = 0.0001;

        ArrayList<ArrayList<Object>> result = new ArrayList<>();
//        if(dataset.getTableName() != null){
//        System.out.println(Table.getMappingJSON());
        JSONObject tableJSON = (JSONObject) Table.getMappingJSON().get(dataset.getTableName());
//        } else {
//            ArrayList<String> head = new ArrayList<>(Arrays.asList(SparkExecutor.headers.get(dataset.getTableName())));
//            int colIndex = head.indexOf(val1);
//        }
        if(operator.equals("=") || operator.equals("==")) {
            for(ArrayList<Object> row: dataset.table){
//                if(val2.getClass().getSimpleName().equals("Integer")){
//                    Integer val2Int = Integer.parseInt(val2);
//                    if(row.get((int) tableJSON.get(val1)).equals(val2Int)) {
//                        result.add(row);
//                    }
//                } else {
//                    if(row.get((int) tableJSON.get(val1)).equals(val2)) {
//                        result.add(row);
//                    }
//                }
                if(isNumeric(val2)){
                    if(row.get((int) tableJSON.get(val1)).equals(Integer.parseInt(val2))){
                        result.add(row);
                    }
                } else {
//                    System.out.println(val1);
//                    System.out.println(val2);
//                    System.out.println(dataset.getTableName());
                    ArrayList<String> colsForJoin = new ArrayList<>();
                    for(String col: SparkExecutor.headers.get(dataset.getTableName())){
                        colsForJoin.add(col);
                    };
//                    System.out.println(colsForJoin);
                    //ArrayList<String> aListColsForJoin = new ArrayList<>(Arrays.asList(colsForJoin));
                    //int indexOfVal1 = aListColsForJoin.indexOf(val1);
                    int indexOfVal1 = colsForJoin.indexOf(val1);
//                    System.out.println(indexOfVal1);
//                    System.out.println(row.get(indexOfVal1));
                    if(((String) row.get(indexOfVal1)).equalsIgnoreCase(val2)){
                        result.add(row);
                    }
                }
//                System.out.println("row.size(): " + row.size());
//                System.out.println("result.size(): " + result.size());
            }
        }
        else if(operator.equalsIgnoreCase("!=") || operator.equalsIgnoreCase("<>")) {
            for(ArrayList<Object> row: dataset.table){
                if(isNumeric(val2) && row.get((int) tableJSON.get(val1)).equals(Integer.parseInt(val2))) {
                    result.add(row);
                }
                else if(!isNumeric(val2) && row.get((int) tableJSON.get(val1)).equals(val2)){
                    result.add(row);
                }
            }
        }
        else if(operator.equalsIgnoreCase(">=")) {
            for (ArrayList<Object> row : dataset.table) {
                int actualValue = (int) row.get((int) tableJSON.get(val1));
                int compareValue = Integer.parseInt(val2);
                if (Math.abs(actualValue - compareValue) < epsilon || actualValue > compareValue) {
                    result.add(row);
                }
            }
        }
        else if(operator.equalsIgnoreCase("<=")) {
            for (ArrayList<Object> row : dataset.table) {
                int actualValue = (int) row.get((int) tableJSON.get(val1));
                int compareValue = Integer.parseInt(val2);
                if (Math.abs(actualValue - compareValue) < epsilon || actualValue < compareValue) {
                    result.add(row);
                }
            }
        }
        else if(operator.equalsIgnoreCase(">")) {
            for (ArrayList<Object> row : dataset.table) {
                int actualValue = (int) row.get((int) tableJSON.get(val1));
                int compareValue = Integer.parseInt(val2);
                if (actualValue > compareValue) {
                    result.add(row);
                }
            }
        }
        else if(operator.equalsIgnoreCase("<")) {
            for (ArrayList<Object> row : dataset.table) {
                int actualValue = (int) row.get((int) tableJSON.get(val1));
                int compareValue = Integer.parseInt(val2);
                if (actualValue < compareValue) {
                    result.add(row);
                }
            }
        }
        else if(operator.equalsIgnoreCase("LIKE")) {
            for(ArrayList<Object> row : dataset.table) {
                String actualValue = (String) row.get((int) tableJSON.get(val1));
                Pattern pattern = Pattern.compile(val2);
                Matcher matcher = pattern.matcher(val1);
                if(matcher.matches()){
                    result.add(row);
                }
            }
        }
        else if(operator.equalsIgnoreCase("IN")) {
            for(ArrayList<Object> row : dataset.table) {
                String actualValue = (String) row.get((int) tableJSON.get(val1));
                if(val2Array.contains(actualValue)){
                    result.add(row);
                }
            }
        }

        dataset.setTable(result);
    }

    public static void groupBy(JSONObject queryJSON, Table dataset){
        HashMap<String, ArrayList<ArrayList<Object>>> groupByMap = new HashMap<>();
        JSONArray temp = (JSONArray) queryJSON.get("groupByColumns");
        ArrayList<String> groupByColumns = new ArrayList<>();
        for(int i = 0; i < temp.length(); i++){
            groupByColumns.add((String) temp.get(i));
        }

        JSONObject tableJSON = (JSONObject) Table.getMappingJSON().get(dataset.getTableName());
        int colIndex = (int) tableJSON.get(groupByColumns.get(0));
        for(ArrayList<Object> row: dataset.table){
            if(groupByMap.containsKey(row.get(colIndex))){
                groupByMap.get(row.get(colIndex)).add(row);
            } else {
                ArrayList<ArrayList<Object>> tempTable = new ArrayList<>();
                tempTable.add(row);
                groupByMap.put((String) row.get(colIndex), tempTable);
            }
        }
        dataset.setGroupByMap(groupByMap);
//        for(String s: groupByMap.keySet()){
//            System.out.println(groupByMap.get(s));
//        }
    }

    public static void having(JSONObject queryJSON, Table dataset){
        JSONObject havingJSON= (JSONObject) queryJSON.get("having");
        String aggFunc = (String) havingJSON.get("function");
        String aggColumn = (String) havingJSON.get("column");
        String aggOperator = (String) havingJSON.get("operator");
        String aggValue = (String) havingJSON.get("value");

        HashMap<String, Integer> aggMap = aggregate(aggFunc, aggColumn, dataset);

        if(aggOperator.equals("=") || aggOperator.equals("==")) {
            for(Map.Entry<String, Integer> entry: aggMap.entrySet()){
                if(entry.getValue() != Integer.parseInt(aggValue)){
                    dataset.getGroupByMap().remove(entry.getKey());
                }
            }
        }
        else if(aggOperator.equalsIgnoreCase("!=") || aggOperator.equalsIgnoreCase("<>")) {
            for(Map.Entry<String, Integer> entry: aggMap.entrySet()){
                if(entry.getValue() == Integer.parseInt(aggValue)){
                    dataset.getGroupByMap().remove(entry.getKey());
                }
            }
        }
        else if(aggOperator.equalsIgnoreCase(">=")) {
            for(Map.Entry<String, Integer> entry: aggMap.entrySet()){
                if(entry.getValue() < Integer.parseInt(aggValue)){
                    dataset.getGroupByMap().remove(entry.getKey());
                }
            }
        }
        else if(aggOperator.equalsIgnoreCase("<=")) {
            for(Map.Entry<String, Integer> entry: aggMap.entrySet()){
                if(entry.getValue() > Integer.parseInt(aggValue)){
                    dataset.getGroupByMap().remove(entry.getKey());
                }
            }
        }
        else if(aggOperator.equalsIgnoreCase(">")) {
            ArrayList<String> toRemove = new ArrayList<>();
            for(Map.Entry<String, Integer> entry: aggMap.entrySet()){
                if(entry.getValue() <= Integer.parseInt(aggValue)){
                    dataset.getGroupByMap().remove(entry.getKey());
                    toRemove.add(entry.getKey());
                }
            }
            for(String key: toRemove) {
                aggMap.remove(key);
            }
        }
        else if(aggOperator.equalsIgnoreCase("<")) {
            for(Map.Entry<String, Integer> entry: aggMap.entrySet()){
                if(entry.getValue() >= Integer.parseInt(aggValue)){
                    dataset.getGroupByMap().remove(entry.getKey());
                }
            }
        }

//        ArrayList<ArrayList<Object>> table = new ArrayList<>();

//        for(Map.Entry<String, Integer> entry: aggMap.entrySet()){
//            ArrayList<Object> arr = new ArrayList<>();
//            arr.add(entry.getKey());
//            arr.add(entry.getValue());
//            table.add(arr);
//        }
        dataset.aggMap = aggMap;
//        dataset.setTable(table);
    }

    public static Table join(JSONObject queryJSON, Table dataset1, Table dataset2){
        String joinType = (String) queryJSON.get("joinType");
        JSONObject onJSON = (JSONObject) queryJSON.get("on");
        String cond1 = (String) onJSON.get("condition1");
        String cond2 = (String) onJSON.get("condition2");
//        System.out.println(cond1);
//        System.out.println((cond1.split("\\.")).length);
        String col = cond1.split("\\.")[1];
        String table1 = cond1.split("\\.")[0];
        String table2 = cond2.split("\\.")[0];
        int colIndex1 = (int) ((JSONObject) Table.getMappingJSON().get(table1)).get(col);
        int colIndex2 = (int) ((JSONObject) Table.getMappingJSON().get(table2)).get(col);
        Table result = new Table();
        result.setTableName((String) table1+"X"+table2);
        switch(joinType){
            case "natural":
                //iterate over table1 and add matching rows from table 2 to new table
                for(ArrayList<Object> row: dataset1.table) {
                    for(ArrayList<Object> row2: dataset2.table) {
                        if(row.get(colIndex1).equals(row2.get(colIndex2))) {
                            ArrayList<Object> newRow = new ArrayList<>();
                            newRow.addAll(row);
                            newRow.remove(colIndex1);
                            newRow.addAll(row2);
                            result.table.add(newRow);
                        }
                    }
                }
                break;
            case "inner":
                //iterate over table1 and add matching rows from table 2 to new table
               for(ArrayList<Object> row: dataset1.table) {
                    for(ArrayList<Object> row2: dataset2.table) {
                        if(row.get(colIndex1).equals(row2.get(colIndex2))) {
                            ArrayList<Object> newRow = new ArrayList<>();
                            newRow.addAll(row);
                            //newRow.remove(colIndex1);
                            newRow.addAll(row2);
                            result.table.add(newRow);
                        }
                    }
                }
                break;
            case "left":
                //iterate over table1 and add row elems for rest add null
                for(ArrayList<Object> row: dataset1.table) {
                    for(ArrayList<Object> row2: dataset2.table) {
                        if(row.get(colIndex1).equals(row2.get(colIndex2))) {
                            ArrayList<Object> newRow = new ArrayList<>();
                            newRow.addAll(row);
                            //newRow.remove(colIndex1);
                            newRow.addAll(row2);
                            result.table.add(newRow);
                        }
                        else {
                            ArrayList<Object> newRow = new ArrayList<>();
                            newRow.addAll(row);
                            for(int i=0; i<row2.size()-1; i++) {
                                newRow.add("null");
                            }
                            result.table.add(newRow);
                        }
                    }
                }
                break;
            case "right":
                for(ArrayList<Object> row: dataset2.table) {
                    for(ArrayList<Object> row2: dataset1.table) {
                        if(row.get(colIndex1).equals(row2.get(colIndex2))) {
                            ArrayList<Object> newRow = new ArrayList<>();
                            newRow.addAll(row);
                            //newRow.remove(colIndex1);
                            newRow.addAll(row2);
                            result.table.add(newRow);
                        }
                        else {
                            ArrayList<Object> newRow = new ArrayList<>();
                            for(int i=0; i<row.size()-1; i++) {
                                newRow.add("null");
                            }
                            newRow.addAll(row2);
                            result.table.add(newRow);
                        }
                    }
                }
                break;
            case "outer":
                for(ArrayList<Object> row: dataset1.table) {
                    for(ArrayList<Object> row2: dataset2.table) {
                        if(row.get(colIndex1).equals(row2.get(colIndex2))) {
                            ArrayList<Object> newRow = new ArrayList<>();
                            newRow.addAll(row);
                            //newRow.remove(colIndex1);
                            newRow.addAll(row2);
                            result.table.add(newRow);
                        } else {
                            ArrayList<Object> newRow = new ArrayList<>();
                            for(int i=0; i<row.size()-1; i++) {
                                newRow.add("null");
                            }
                            newRow.addAll(row2);
                            result.table.add(newRow);
                            ArrayList<Object> newRow2 = new ArrayList<>();
                            newRow2.addAll(row);
                            for(int i=0; i<row2.size()-1; i++) {
                                newRow2.add("null");
                            }
                            result.table.add(newRow2);
                        }
                    }
                }
                break;
            default:
                System.out.println("kuch bt idk fuck this");
        }

        return result;
    }

    public static HashMap<String, Integer> aggregate(String aggFunc, String aggColumn, Table dataset){
        //having case
        HashMap<String, ArrayList<ArrayList<Object>>> groupByMap = dataset.getGroupByMap();
        HashMap<String, Integer> aggMap = new HashMap<>();
        JSONObject tableJSON = (JSONObject) Table.getMappingJSON().get(dataset.getTableName());
        int aggColIndex = (int) tableJSON.get(aggColumn);
        switch(aggFunc.toLowerCase()) {
            case "count" :
                for(Map.Entry<String, ArrayList<ArrayList<Object>>> entry: groupByMap.entrySet()) {
                    aggMap.put(entry.getKey(), entry.getValue().size());
                }
                break;
            case "sum" :
                for(Map.Entry<String, ArrayList<ArrayList<Object>>> entry: groupByMap.entrySet()) {
                    int sum = 0;
                    for(ArrayList<Object> row : entry.getValue()) {
                        sum += (int) row.get(aggColIndex);
                    }
                    aggMap.put(entry.getKey(), sum);
                }
                break;
            case "max":
                for(Map.Entry<String, ArrayList<ArrayList<Object>>> entry: groupByMap.entrySet()) {
                    int sum = 0, max = 0;
                    for(ArrayList<Object> row : entry.getValue()) {
                        max = Math.max((int) row.get(aggColIndex), max);
                    }
                    aggMap.put(entry.getKey(), max);
                }
                break;
            case "min":
                for(Map.Entry<String, ArrayList<ArrayList<Object>>> entry: groupByMap.entrySet()) {
                    int sum = 0, min = Integer.MAX_VALUE;
                    for(ArrayList<Object> row : entry.getValue()) {
                        min = Math.max((int) row.get(aggColIndex), min);
                    }
                    aggMap.put(entry.getKey(), min);
                }
                break;
            case "avg":
                for(Map.Entry<String, ArrayList<ArrayList<Object>>> entry: groupByMap.entrySet()) {
                    int sum = 0;
                    for(ArrayList<Object> row : entry.getValue()) {
                        sum += (int) row.get(aggColIndex);
                    }
                    aggMap.put(entry.getKey(), sum/entry.getValue().size());
                }
                break;
            default:
                System.out.println("\nInvalid aggregate function.");
        }
        return aggMap;
    }

    public static boolean isNumeric(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch(NumberFormatException e){
            return false;
        }
    }
}