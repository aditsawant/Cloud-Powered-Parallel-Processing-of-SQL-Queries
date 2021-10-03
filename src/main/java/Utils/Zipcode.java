package Utils;

import java.util.ArrayList;

public class Zipcode extends Table {
    private String zipcode,zipcodetype,city,state;

    public Zipcode(String zipcode, String zipcodetype, String city, String state) {
        this.zipcode = zipcode;
        this.zipcodetype = zipcodetype;
        this.city = city;
        this.state = state;
    }

    public Object getColumnValue(String columnName) {
        if(columnName.equalsIgnoreCase("zipcode")) {
            return this.zipcode;
        }
        else if(columnName.equalsIgnoreCase("zipcodetype")) {
            return this.zipcodetype;
        }
        else if(columnName.equalsIgnoreCase("city")) {
            return this.city;
        }
        else if(columnName.equalsIgnoreCase("state")) {
            return this.state;
        }
        return null;
    }
    public String groupByString(String[] columns) {
        String res = "";
        for(String columnName : columns) {
            if(columnName.equalsIgnoreCase("zipcode")) {
                res = res.concat(this.zipcode).concat("_");
            }
            else if(columnName.equalsIgnoreCase("zipcodetype")) {
                res = res.concat(this.zipcodetype).concat("_");
            }
            else if(columnName.equalsIgnoreCase("city")) {
                res = res.concat(this.city).concat("_");
            }
            else if(columnName.equalsIgnoreCase("state")) {
                res = res.concat(this.state).concat("_");
            }
        }
        return res;
    }
    public Boolean checkColumnValue(String columnName, String value) {
        if(columnName.equalsIgnoreCase("zipcode")) {
            return this.zipcode.equalsIgnoreCase(value);
        }
        else if(columnName.equalsIgnoreCase("zipcodetype")) {
            return this.zipcodetype.equalsIgnoreCase(value);
        }
        else if(columnName.equalsIgnoreCase("city")) {
            return this.city.equalsIgnoreCase(value);
        }
        else if(columnName.equalsIgnoreCase("state")) {
            return this.state.equalsIgnoreCase(value);
        }
        return null;
    }
    public Object getAggregate(String operation, String column, ArrayList<Table> arr) {
        if(operation.equalsIgnoreCase("count")) {
            return arr.size();
        }
        else if(column.equalsIgnoreCase("zipcode") || column.equalsIgnoreCase("zipcodetype") || column.equalsIgnoreCase("city") || column.equalsIgnoreCase("state")) {
            return null;
        }
        else if(operation.equalsIgnoreCase("sum")) {
            int sum = 0;
            for(Table r : arr) {
                sum += (Integer)r.getColumnValue(column);
            }
            return sum;
        }
        else if(operation.equalsIgnoreCase("max")) {
            int maxVal = Integer.MIN_VALUE;
            for(Table r : arr) {
                maxVal = Integer.max(maxVal, (Integer)r.getColumnValue(column));
            }
            return maxVal;
        }
        else if(operation.equalsIgnoreCase("min")) {
            int minVal = Integer.MAX_VALUE;
            for(Table r : arr) {
                minVal = Integer.min(minVal, (Integer)r.getColumnValue(column));
            }
            return minVal;
        }

        return null;
    }
    public Boolean compareAggregate(String column, String operation, String comparisonOperator, String value, ArrayList<Table> arr) {
        Double actualValue = Double.parseDouble(getAggregate(operation, column, arr).toString());
        Double toCompareValue = Double.parseDouble(value);
        Double epsilon = 0.0001;
        System.out.println("comparing aggregates ==============");
        System.out.println(actualValue);
        System.out.println(toCompareValue);
        if(comparisonOperator.equalsIgnoreCase("==")) {
            return Math.abs(actualValue - toCompareValue) < epsilon;
        }
        else if(comparisonOperator.equalsIgnoreCase("!=")) {
            return actualValue != toCompareValue;
        }
        else if(comparisonOperator.equalsIgnoreCase(">=")) {
            return (Math.abs(actualValue - toCompareValue) < epsilon) || actualValue > toCompareValue;
        }
        else if(comparisonOperator.equalsIgnoreCase("<=")) {
            return (Math.abs(actualValue - toCompareValue) < epsilon) || actualValue < toCompareValue;
        }
        else if(comparisonOperator.equalsIgnoreCase(">")) {
            return actualValue > toCompareValue;
        }
        else if(comparisonOperator.equalsIgnoreCase("<")) {
            return actualValue < toCompareValue;
        }
        return null;
    }
}
