package Utils;

public class TableFactory {
    public static Table getTable(String tableName, String row) {
//		System.out.println(row);
        String[] values = row.split(",");
//		System.out.println("Inside Factory:=======>");

        if(tableName.equalsIgnoreCase("movies")) {
            if(values.length < 22)
                return null;
            return new Movie(values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7], values[8], values[9], values[10], values[11], values[12], values[13], values[14], values[15], values[16], values[17], values[18], values[19], values[20], values[21]);
        }
        else if(tableName.equalsIgnoreCase("rating")) {
            if(values.length < 4)
                return null;
            return new Rating(values[0], values[1], values[2], values[3]);
        }
        else if(tableName.equalsIgnoreCase("users")) {
            if(values.length < 5)
                return null;
            return new User(values[0], values[1], values[2], values[3], values[4]);
        }
        else if(tableName.equalsIgnoreCase("zipcodes")) {
            if(values.length < 4)
                return null;
            return new Zipcode(values[0], values[1], values[2], values[3]);
        }
        return null;
    }
}
