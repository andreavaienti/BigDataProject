package utils.parser;

public class MetaRecordParser {

    public static boolean areParsable(String row){
        final String[] metaAttributes = row.split(",", -1);
        if(metaAttributes.length != 2)
            return false;
        if(metaAttributes[0].length() < 1 || metaAttributes[1].length() < 1)
            return false;
        return true;
    }

}
