package utils;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class RegexFilter extends Configured implements PathFilter {

    Configuration conf;
    FileSystem fs;

    public boolean accept(Path path) {

        try {
            if (fs.isDirectory(path)) {
                return true;
            } else {
                return path.getName().endsWith(conf.get("file.pattern"));
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        if (conf != null) {
            try {
                fs = FileSystem.get(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}