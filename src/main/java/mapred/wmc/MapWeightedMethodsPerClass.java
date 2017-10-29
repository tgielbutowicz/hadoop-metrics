package mapred.wmc;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapWeightedMethodsPerClass extends Mapper<Text, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String fileContents = value.toString().trim();
        Pattern tagPattern = Pattern
                .compile("(public|protected|private|static|\\s) +[\\w\\<\\>\\[\\]]+\\s+(\\w+) *\\([^\\)]*\\) *(\\{?|[^;])");
        Matcher tagMatcher = tagPattern.matcher(fileContents);
        while (tagMatcher.find()) {
            context.write(key, one);
        }
    }
}
// WMC Weighted Methods Per Class
//
// Despite its long name, WMC is simply the method count for a class.
// WMC = number of methods defined in class
//
// Keep WMC down. A high WMC has been found to lead to more faults. Classes with
// many methods are likely to be more more application specific, limiting the
// possibility of reuse. WMC is a predictor of how much time and effort is
// required to develop and maintain the class. A large number of methods also
// means a greater potential impact on derived classes, since the derived
// classes inherit (some of) the methods of the base class. Search for high WMC
// values to spot classes that could be restructured into several smaller
// classes.
//
// What is a good WMC? Different limits have been defined. One way is to limit
// the number of methods in a class to, say, 20 or 50. Another way is to specify
// that a maximum of 10% of classes can have more than 24 methods. This allows
// large classes but most classes should be small.
//
// A study of 30 C++ projects suggests that an increase in the average WMC
// increases the density of bugs and decreases quality. The study suggests
// "optimal" use for WMC but doesn't tell what the optimum range is. It sounds
// safe to assume that a high WMC is detrimental in VB as well. Misra & Bhavsar:
// Relationships Between Selected Software Measures and Latent Bug-Density:
// Guidelines for Improving Quality. Springer-Verlag 2003.
//
// Implementation details. Project Analyzer counts each Sub, Function, Operator
// and Property accessor into WMC. Constructors and event handlers are also
// counted as methods. Event definitions, Custom Events, API Declare statements
// and <DLLImport> procedures are not counted in WMC. WMC includes only those
// methods that are defined in the class, not any inherited methods. Overriding
// and shadowing methods defined in the class are counted, since they form a new
// implementation of a method.
//
// Each property accessor (Get, Set, Let) is counted separately in WMC. The
// reasoning behind this is that each of Get, Set and Let provides a separate
// way to access to the underlying property and is essentially a method of the
// class. The alternative way to using properties would be to write accessor
// functions: Function getProperty and Sub setProperty. Both of these would also
// be counted in WMC the same way we count each property accessor.
//
// Even though Project Analyzer counts WMC as a simple method count, it could be
// a weighted count. In principle, we could weigh each method by its size or
// complexity. This is not implemented, though.
//
// Readings
//
// Shyam R. Chidamber, Chris F. Kemerer. A Metrics suite for Object Oriented
// design. M.I.T. Sloan School of Management E53-315. 1993.
// http://uweb.txstate.edu/~mg43/CS5391/Papers/Metrics/OOMetrics.pdf
// Victor Basili, Lionel Briand and Walcelio Melo. A Validation of
// Object-Oriented Design Metrics as Quality Indicators. IEEE Transactions on
// Software Engineering. Vol. 22, No. 10, October 1996.
// http://www.cs.umd.edu/users/basili/publications/journals/J60.pdf
