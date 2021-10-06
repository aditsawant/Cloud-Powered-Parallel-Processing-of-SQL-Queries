package Hadoop;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Locale;

public class Movie extends Table {
    private Integer movieid;
    private String title;
    private LocalDate releasedate;
    private Integer unknown;
    private Integer Action;
    private Integer Adventure;
    private Integer Animation;
    private Integer Children;
    private Integer Comedy;
    private Integer Crime;
    private Integer Documentary;
    private Integer Drama;
    private Integer Fantasy;
    private Integer Film_Noir;
    private Integer Horror;
    private Integer Musical;
    private Integer Mystery;
    private Integer Romance;
    private Integer Sci_Fi;
    private Integer Thriller;
    private Integer War;
    private Integer Western;

    String[] genreList = {"unknown", "Action", "Adventure", "Animation", "Children",
            "Comedy", "Crime", "Documentary", "Drama", "Fantasy",
            "Film_Noir", "Horror", "Musical", "Mystery", "Romance",
            "Sci_Fi", "Thriller", "War", "Western"};

    public Movie(String movieid, String title, String releasedate, String unknown, String action, String adventure, String animation, String children, String comedy, String crime, String documentary, String drama, String fantasy, String film_Noir, String horror, String musical, String mystery, String romance, String sci_Fi, String thriller, String war, String western) {
        this.movieid = Integer.parseInt(movieid);
        this.title = title;
        releasedate=releasedate.substring(0,4)+releasedate.substring(4,6).toLowerCase()+releasedate.substring(6);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MMM-yy", Locale.ENGLISH);
        this.releasedate = LocalDate.parse(releasedate, formatter);
        this.unknown = Integer.parseInt(unknown);
        Action = Integer.parseInt(action);
        Adventure = Integer.parseInt(adventure);
        Animation = Integer.parseInt(animation);
        Children = Integer.parseInt(children);
        Comedy = Integer.parseInt(comedy);
        Crime = Integer.parseInt(crime);
        Documentary = Integer.parseInt(documentary);
        Drama = Integer.parseInt(drama);
        Fantasy = Integer.parseInt(fantasy);
        Film_Noir = Integer.parseInt(film_Noir);
        Horror = Integer.parseInt(horror);
        Musical = Integer.parseInt(musical);
        Mystery = Integer.parseInt(mystery);
        Romance = Integer.parseInt(romance);
        Sci_Fi = Integer.parseInt(sci_Fi);
        Thriller = Integer.parseInt(thriller);
        War = Integer.parseInt(war);
        Western = Integer.parseInt(western);
    }

    public Movie()
        {
        this.movieid = -1;
        this.title = "null";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MMM-yy", null);
        this.releasedate = null;
        this.unknown = -1;
        Action = -1;
        Adventure = -1;
        Animation = -1;
        Children = -1;
        Comedy = -1;
        Crime = -1;
        Documentary = -1;
        Drama = -1;
        Fantasy = -1;
        Film_Noir = -1;
        Horror = -1;
        Musical = -1;
        Mystery = -1;
        Romance = -1;
        Sci_Fi = -1;
        Thriller = -1;
        War = -1;
        Western = -1;
    }
    public static Integer getNumColumns()
    {
        return 22;
    }

    public String[] getColumnList()
    {
        String [] clist= new String[22];
        clist[0]="movieid";
        clist[1]="title";
        clist[2]="releasedate";
        for(int i=3;i<22;++i)
        {
            clist[i]=genreList[i-3];
        }
        return clist;
    }
    public Object getColumnValue(String columnName){
        if(columnName.equalsIgnoreCase("movieid")){
            return this.movieid;
        }
        if(columnName.equalsIgnoreCase("title")){
            return this.title;
        }
        if(columnName.equalsIgnoreCase("releasedate")){
            if(this.releasedate==null)
                return "null";
            return this.releasedate;
        }
        if(columnName.equalsIgnoreCase("unknown")){
            if(this.unknown==-1)
                return "null";
            return this.unknown;
        }
        if(columnName.equalsIgnoreCase("Action")){
            if(this.Action==-1)
                return "null";
            return this.Action;
        }
        if(columnName.equalsIgnoreCase("Adventure")){
            if(this.Adventure==-1)
                return "null";
            return this.Adventure;
        }
        if(columnName.equalsIgnoreCase("Animation")){
            if(this.Animation==-1)
                return "null";
            return this.Animation;
        }if(columnName.equalsIgnoreCase("Children")){
            return this.Children;
        }
        if(columnName.equalsIgnoreCase("Comedy")){
            if(this.Comedy==-1)
                return "null";
            return this.Comedy;
        }
        if(columnName.equalsIgnoreCase("Crime")){
            if(this.Crime==-1)
                return "null";
            return this.Crime;
        }
        if(columnName.equalsIgnoreCase("Documentary")){
            if(this.Documentary==-1)
                return "null";
            return this.Documentary;
        }
        if(columnName.equalsIgnoreCase("Drama")){
            if(this.Drama==-1)
                return "null";
            return this.Drama;
        }
        if(columnName.equalsIgnoreCase("Fantasy")){
            if(this.Fantasy==-1)
            return "null";
            return this.Fantasy;
        }
        if(columnName.equalsIgnoreCase("Film_Noir")){
            if(this.Film_Noir==-1)
                return "null";
            return this.Film_Noir;
        }
        if(columnName.equalsIgnoreCase("Horror")){
            if(this.Horror==-1)
                return "null";
            return this.Horror;
        }
        if(columnName.equalsIgnoreCase("Musical")){
            if(this.Musical==-1)
                return "null";
            return this.Musical;
        }
        if(columnName.equalsIgnoreCase("Mystery")){
            if(this.Mystery==-1)
            return "null";
            return this.Mystery;
        }
        if(columnName.equalsIgnoreCase("Romance")){
            if(this.Romance==-1)
                return "null";
            return this.Romance;
        }
        if(columnName.equalsIgnoreCase("Sci_Fi")){
            if(this.Sci_Fi==-1)
                return "null";
            return this.Sci_Fi;
        }
        if(columnName.equalsIgnoreCase("Thriller")){
            if(this.Thriller==-1)
                return "null";
            return this.Thriller;
        }
        if(columnName.equalsIgnoreCase("War")){
            if(this.War==-1)
                return "null";
            return this.War;
        }
        if(columnName.equalsIgnoreCase("Western")){
            if(this.Western==-1)
                return "null";
            return this.Western;
        }
        return null;
    }

    public Boolean checkColumnValue(String columnName, String value){
        if(columnName.equalsIgnoreCase("movieId")){
            return Integer.parseInt(value) == this.movieid;
        }
        if(columnName.equalsIgnoreCase("movieTitle")){
            return this.title.equalsIgnoreCase(value);
        }
        if(columnName.equalsIgnoreCase("releaseDate")){
            return this.releasedate.isEqual(LocalDate.parse(value));
        }
        if(columnName.equalsIgnoreCase("unknown")){
            return Integer.parseInt(value) == this.unknown;
        }
        if(columnName.equalsIgnoreCase("Action")){
            return Integer.parseInt(value) == this.Action;
        }
        if(columnName.equalsIgnoreCase("Adventure")){
            return Integer.parseInt(value) == this.Adventure;
        }
        if(columnName.equalsIgnoreCase("Animation")){
            return Integer.parseInt(value) == this.Animation;
        }
        if(columnName.equalsIgnoreCase("Children")){
            return Integer.parseInt(value) == this.Children;
        }
        if(columnName.equalsIgnoreCase("Comedy")){
            return Integer.parseInt(value) == this.Comedy;
        }
        if(columnName.equalsIgnoreCase("Crime")){
            return Integer.parseInt(value) == this.Crime;
        }
        if(columnName.equalsIgnoreCase("Documentary")){
            return Integer.parseInt(value) == this.Documentary;
        }
        if(columnName.equalsIgnoreCase("Drama")){
            return Integer.parseInt(value) == this.Drama;
        }
        if(columnName.equalsIgnoreCase("Fantasy")){
            return Integer.parseInt(value) == this.Fantasy;
        }
        if(columnName.equalsIgnoreCase("Film_Noir")){
            return Integer.parseInt(value) == this.Film_Noir;
        }
        if(columnName.equalsIgnoreCase("Horror")){
            return Integer.parseInt(value) == this.Horror;
        }
        if(columnName.equalsIgnoreCase("Musical")){
            return Integer.parseInt(value) == this.Musical;
        }
        if(columnName.equalsIgnoreCase("Mystery")){
            return Integer.parseInt(value) == this.Mystery;
        }
        if(columnName.equalsIgnoreCase("Romance")){
            return Integer.parseInt(value) == this.Romance;
        }
        if(columnName.equalsIgnoreCase("Sci_Fi")){
            return Integer.parseInt(value) == this.Sci_Fi;
        }
        if(columnName.equalsIgnoreCase("Thriller")){
            return Integer.parseInt(value) == this.Thriller;
        }
        if(columnName.equalsIgnoreCase("War")){
            return Integer.parseInt(value) == this.War;
        }
        if(columnName.equalsIgnoreCase("Western")){
            return Integer.parseInt(value) == this.Western;
        }
        return null;
    }

    public String groupByString(String[] columns) {
        String res = "";
        for(String columnName : columns) {
            if(columnName.equalsIgnoreCase("movieid")) {
                res = res.concat(this.movieid.toString()).concat("_");
            }
            else if(columnName.equalsIgnoreCase("title")) {
                res = res.concat(this.title).concat("_");
            }
            else if(columnName.equalsIgnoreCase("releasedate")) {
                res = res.concat(this.releasedate.toString()).concat("_");
            }
            else if(columnName.equalsIgnoreCase("unknown")) {
                res = res.concat(this.unknown.toString()).concat("_");
            }
            else if(columnName.equalsIgnoreCase("Action")) {
                res = res.concat(this.Action.toString()).concat("_");
            }
            else if(columnName.equalsIgnoreCase("Adventure")) {
                res = res.concat(this.Adventure.toString()).concat("_");
            }
            else if(columnName.equalsIgnoreCase("Animation")) {
                res = res.concat(this.Animation.toString()).concat("_");
            }
            else if(columnName.equalsIgnoreCase("Children")) {
                res = res.concat(this.Children.toString()).concat("_");
            }
            else if(columnName.equalsIgnoreCase("Comedy")) {
                res = res.concat(this.Comedy.toString()).concat("_");
            }
            else if(columnName.equalsIgnoreCase("Crime")) {
                res = res.concat(this.Crime.toString()).concat("_");
            }
            else if(columnName.equalsIgnoreCase("Documentary")) {
                res = res.concat(this.Documentary.toString()).concat("_");
            }
            else if(columnName.equalsIgnoreCase("Drama")) {
                res = res.concat(this.Drama.toString()).concat("_");
            }
            else if(columnName.equalsIgnoreCase("Fantasy")) {
                res = res.concat(this.Fantasy.toString()).concat("_");
            }
            else if(columnName.equalsIgnoreCase("Film_Noir")) {
                res = res.concat(this.Film_Noir.toString()).concat("_");
            }
            else if(columnName.equalsIgnoreCase("Horror")) {
                res = res.concat(this.Horror.toString()).concat("_");
            }
            else if(columnName.equalsIgnoreCase("Musical")) {
                res = res.concat(this.Musical.toString()).concat("_");
            }
            else if(columnName.equalsIgnoreCase("Mystery")) {
                res = res.concat(this.Mystery.toString()).concat("_");
            }
            else if(columnName.equalsIgnoreCase("Romance")) {
                res = res.concat(this.Romance.toString()).concat("_");
            }
            else if(columnName.equalsIgnoreCase("Sci_Fi")) {
                res = res.concat(this.Sci_Fi.toString()).concat("_");
            }
            else if(columnName.equalsIgnoreCase("Thriller")) {
                res = res.concat(this.Thriller.toString()).concat("_");
            }
            else if(columnName.equalsIgnoreCase("War")) {
                res = res.concat(this.War.toString()).concat("_");
            }
            else if(columnName.equalsIgnoreCase("Western")) {
                res = res.concat(this.Western.toString()).concat("_");
            }
        }
        return res;
    }

    public Object getAggregate(String operation, String column, ArrayList<Table> arr) {
        if(operation.equalsIgnoreCase("count")) {
            return arr.size();
        }
        else if(column.equalsIgnoreCase("title") || column.equalsIgnoreCase("releasedate")) {
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
