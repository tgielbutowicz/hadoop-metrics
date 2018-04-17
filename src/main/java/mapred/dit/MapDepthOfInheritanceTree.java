package mapred.dit;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapDepthOfInheritanceTree extends Mapper<Text, Text, Text, Text> {
    private final static Text cls = new Text();
    private final static Text supercls = new Text();

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String fileContents = value.toString().trim();
//        Pattern classPattern = Pattern.compile("(?<=\\bextends\\s)(\\w+)");
        Pattern classPattern = Pattern.compile("\\s*(public|private)\\s+class\\s+(\\w+)");
        Pattern superclassPattern = Pattern.compile("\\s+(extends\\s+)+(\\w+)");
        Matcher classMatcher = classPattern.matcher(fileContents);
        Matcher superclassMatcher = superclassPattern.matcher(fileContents);
        while (classMatcher.find()) {
            cls.set(classMatcher.group(2));
            if (superclassMatcher.find()) {
                supercls.set(superclassMatcher.group(2));
            } else {
                supercls.set("1");
            }
            context.write(cls, supercls);
        }
    }
}

// DIT Depth of Inheritance Tree
// DIT = maximum inheritance path from the class to the root class
//
// The deeper a class is in the hierarchy, the more methods and variables it is
// likely to inherit, making it more complex. Deep trees as such indicate
// greater design complexity. Inheritance is a tool to manage complexity,
// really, not to not increase it. As a positive factor, deep trees promote
// reuse because of method inheritance.
//
// A high DIT has been found to increase faults. However, it’s not necessarily
// the classes deepest in the class hierarchy that have the most faults.
// Glasberg et al. have found out that the most fault-prone classes are the ones
// in the middle of the tree. According to them, root and deepest classes are
// consulted often, and due to familiarity, they have low fault-proneness
// compared to classes in the middle.
//
// A recommended DIT is 5 or less. The Visual Studio .NET documentation
// recommends that DIT <= 5 because excessively deep class hierachies are
// complex to develop. Some sources allow up to 8.
//
// A study of 30 C++ projects suggests that an increase in DIT increases the
// density of bugs and decreases quality. The study suggests "optimal" use for
// DIT but doesn't tell what the optimum is. It sounds safe to assume that a
// deep inheritance tree is detrimental in VB as well. Misra & Bhavsar:
// Relationships Between Selected Software Measures and Latent Bug-Density:
// Guidelines for Improving Quality. Springer-Verlag 2003.
//
// Implementation details. Project Analyzer takes only implementation
// inheritance (Inherits statement, not Implements statement) into account when
// calculating DIT.
//
// Special cases. When a class inherits directly from System.Object (or has no
// Inherits statement), DIT=1. For a class that inherits from an unknown
// (unanalyzed) class, DIT=2. This is because the unknown class eventually
// inherits from System.Object and 2 is the minimum inheritance depth. It could
// also be more.