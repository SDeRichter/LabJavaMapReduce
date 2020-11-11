package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();
        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("districtwithtrees", DistrictWithTrees.class,
                    "A map/reduce program that output the districts with trees.");
            programDriver.addClass("treespecies", TreeSpecies.class,
                    "A map/reduce program that output the genre of trees.");
            programDriver.addClass("numbertreebyspecies", NumberOfTreesBySpecies.class,
                    "A map/reduce program that output the number of trees by species.");
            programDriver.addClass("maximumheight", MaximumHeight.class,
                    "A map/reduce program that output the maximum height of trees by species.");
            programDriver.addClass("sortheight", SortHeight.class,
                    "A map/reduce program that output the Tree sorted by height for each specie.");
            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        System.exit(exitCode);
    }
}
