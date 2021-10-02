package Utils;

import org.json.JSONArray;
import org.json.JSONObject;

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
        //        String str = null;
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
        ArrayList<String> all_columns = new ArrayList<>(tableJSON.keySet());
        System.out.println(columns.toString());
        System.out.println(all_columns.toString());

        HashMap<String, String[]> headers = new HashMap<>();
        headers.put("movies", new String[]{"movieid", "title", "releasedate", "unknown", "Action", "Adventure", "Animation", "Children", "Comedy", "Crime", "Documentary", "Drama", "Fantasy", "Film_Noir", "Horror", "Musical", "Mystery", "Romance", "Sci_Fi", "Thriller", "War", "Western"});
        headers.put("users", new String[]{"userid", "age", "gender", "occupation", "zipcode"});
        headers.put("zipcodes", new String[]{"zipcode", "zipcodetype", "city", "state"});
        headers.put("rating", new String[]{"userid", "movieid", "rating", "timestamp"});

        ArrayList<String> columnNames = new ArrayList<>(Arrays.asList(headers.get((String) queryJSON.get("table"))));

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
                else{
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
        JSONObject tableJSON = (JSONObject) Table.getMappingJSON().get(dataset.getTableName());
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
                    if(((String) row.get((int) tableJSON.get(val1))).equalsIgnoreCase(val2)){
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
                } else if(!isNumeric(val2) && row.get((int) tableJSON.get(val1)).equals(val2)){
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
        Table result = new Table();

//        switch(joinType){
//            "inner":
//
//        }

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