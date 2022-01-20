package utils.parser;

import org.apache.commons.lang.math.NumberUtils;

public class CoreRecordParser {

    public static boolean areParsable(String row){
        final String[] metaAttributes = row.split(",", -1);
        if(metaAttributes.length != 4)
            return false;
        if(metaAttributes[0].length() < 1 || metaAttributes[1].length() < 1 || metaAttributes[2].length() < 1 || metaAttributes[3].length() < 1)
            return false;
        if((!NumberUtils.isNumber(metaAttributes[0])) || (!NumberUtils.isNumber(metaAttributes[3])))
            return false;
        return true;
    }
}
