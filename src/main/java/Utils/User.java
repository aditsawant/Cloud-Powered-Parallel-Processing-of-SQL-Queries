package Utils;

import java.util.ArrayList;

public class User extends Table {
    private Integer userid, age;
    private String gender,occupation,zipcode;
    public User(String userid, String age, String gender, String occupation, String zipcode) {
        this.userid = Integer.parseInt(userid);
        this.age = Integer.parseInt(age);
        this.gender = gender;
        this.occupation = occupation;
        this.zipcode = zipcode;
    }
    public Object getColumnValue(String columnName) {
        if(columnName.equalsIgnoreCase("userid")) {
            return this.userid;
        }
        else if(columnName.equalsIgnoreCase("age")) {
            return this.age;
        }
        else if(columnName.equalsIgnoreCase("gender")) {
            return this.gender;
        }
        else if(columnName.equalsIgnoreCase("occupation")) {
            return this.occupation;
        }
        else if(columnName.equalsIgnoreCase("zipcode")) {
            return this.zipcode;
        }
        return null;
    }
    public String groupByString(String[] columns) {
        String res = "";
        for(String columnName : columns) {
            if(columnName.equalsIgnoreCase("userid")) {
                res = res.concat(this.userid.toString()).concat("_");
            }
            else if(columnName.equalsIgnoreCase("age")) {
                res = res.concat(this.age.toString()).concat("_");
            }
            else if(columnName.equalsIgnoreCase("gender")) {
                res = res.concat(this.gender).concat("_");
            }
            else if(columnName.equalsIgnoreCase("occupation")) {
                res = res.concat(this.occupation).concat("_");
            }
            else if(columnName.equalsIgnoreCase("zipcode")) {
                res = res.concat(this.zipcode).concat("_");
            }
        }
        return res;
    }
    public Boolean checkColumnValue(String columnName, String value) {
        if(columnName.equalsIgnoreCase("userid")) {
            return this.userid == Integer.parseInt(value);
        }
        else if(columnName.equalsIgnoreCase("age")) {
            return this.age == Integer.parseInt(value);
        }
        else if(columnName.equalsIgnoreCase("gender")) {
            return this.gender.equalsIgnoreCase(value);
        }
        else if(columnName.equalsIgnoreCase("occupation")) {
            return this.occupation.equalsIgnoreCase(value);
        }
        else if(columnName.equalsIgnoreCase("zipcode")) {
            return this.zipcode.equalsIgnoreCase(value);
        }
        return null;
    }
    public Object getAggregate(String operation, String column, ArrayList<Table> arr) {
        if(operation.equalsIgnoreCase("count")) {
            return arr.size();
        }
        else if(column.equalsIgnoreCase("gender") || column.equalsIgnoreCase("occupation") || column.equalsIgnoreCase("zipcode")) {
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
