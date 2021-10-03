package Utils;

import java.util.ArrayList;

public class Rating extends Table {
    private Integer userid;
    private Integer movieid;
    private Integer rating;
    private Integer timestamp;

    public Rating(String userid, String movieid, String rating, String timestamp) {
        this.userid = Integer.parseInt(userid);
        this.movieid = Integer.parseInt(movieid);
        this.rating = Integer.parseInt(rating);
        this.timestamp = Integer.parseInt(timestamp);
    }
    public Rating()
    {
        this.userid = -1;
        this.movieid = -1;
        this.rating = -1;
        this.timestamp = -1;
    }
    public static Integer getNumColumns()
    {
        return 4;
    }

    public Object getColumnValue(String columnName) {
        if(columnName.equalsIgnoreCase("userid")) {
            return this.userid;
        }
        else if(columnName.equalsIgnoreCase("movieid")) {
            return this.movieid;
        }
        else if(columnName.equalsIgnoreCase("rating")) {
            return this.rating;
        }
        else if(columnName.equalsIgnoreCase("timestamp")) {
            return this.timestamp;
        }
        return null;
    }
    public String groupByString(String[] columns) {
        String res = "";
        for(String columnName : columns) {
            if(columnName.equalsIgnoreCase("userid")) {
                res = res.concat(this.userid.toString()).concat("_");
            }
            else if(columnName.equalsIgnoreCase("movieid")) {
                res = res.concat(this.movieid.toString()).concat("_");
            }
            else if(columnName.equalsIgnoreCase("rating")) {
                res = res.concat(this.rating.toString()).concat("_");
            }
            else if(columnName.equalsIgnoreCase("timestamp")) {
                res = res.concat(this.timestamp.toString()).concat("_");
            }
        }
        return res;
    }
    public Boolean checkColumnValue(String columnName, String value) {
        if(columnName.equalsIgnoreCase("userid")) {
            return this.userid == (int) Integer.parseInt(value);
        }
        else if(columnName.equalsIgnoreCase("movieid")) {
            return this.movieid == (int)Integer.parseInt(value);
        }
        else if(columnName.equalsIgnoreCase("rating")) {
            return this.rating == (int)Integer.parseInt(value);
        }
        else if(columnName.equalsIgnoreCase("timestamp")) {
            return this.timestamp == (int)Integer.parseInt(value);
        }
        return null;
    }
    public Object getAggregate(String operation, String column, ArrayList<Table> arr) {
        if(operation.equalsIgnoreCase("count")) {
            return arr.size();
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
