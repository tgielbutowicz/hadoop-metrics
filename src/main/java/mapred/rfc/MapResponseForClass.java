package mapred.rfc;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapResponseForClass extends Mapper<Text, Text, Text, Text> {
}
// The response set of a class is a set of methods that can potentially be
// executed in response to a message received by an object of that class. RFC is
// simply the number of methods in the set.
// RFC = M + R (First-step measure)
// RFC’ = M + R’ (Full measure)
// M = number of methods in the class
// R = number of remote methods directly called by methods of the class
// R’ = number of remote methods called, recursively through the entire call
// tree
//
// A given method is counted only once in R (and R’) even if it is executed by
// several methods M.
//
// Since RFC specifically includes methods called from outside the class, it is
// also a measure of the potential communication between the class and other
// classes.
//
// A large RFC has been found to indicate more faults. Classes with a high RFC
// are more complex and harder to understand. Testing and debugging is
// complicated. A worst case value for possible responses will assist in
// appropriate allocation of testing time.
//
// A study of 30 C++ projects suggests that an increase in RFC increases the
// density of bugs and decreases quality. The study suggests "optimal" use for
// RFC but doesn't tell what the optimum is.
//
// RFC is the original definition of the measure. It counts only the first level
// of calls outside of the class. RFC’ measures the full response set, including
// methods called by the callers, recursively, until no new remote methods can
// be found. If the called method is polymorphic, all the possible remote
// methods executed are included in R and R’.
//
// The use of RFC’ should be preferred over RFC. RFC was originally defined as a
// first-level metric because it was not practical to consider the full call
// tree in manual calculation. With an automated code analysis tool, getting
// RFC’ values is not longer problematic. As RFC’ considers the entire call tree
// and not just one first level of it, it provides a more thorough measurement
// of the code executed.
//
// Implementation details. Project Analyzer calculates RFC and RFC´ from the
// procedure forward call tree. It regards all subs, functions, properties and
// API declares as methods, whether in classes or other modules. Calls to
// property Set, Let and Get are all counted separately. Calls to VB library
// functions, such as print, are not counted. Calls are counted to declared API
// procedures. If COM libraries were included in the analysis, calls to
// procedures in those libraries are also counted. The counting stops at the API
// call or COM procedure, no recursion is done because the callees of an API or
// COM procedure are unknown. Dead methods and their callees are included in the
// measure. Although they’re not executed at the moment, they could become live
// by a change in the code.
//
// Limitation of implementation. RFC doesn't include calls via RaiseEvent. RFC’
// does.
