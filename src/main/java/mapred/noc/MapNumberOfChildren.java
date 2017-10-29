package mapred.noc;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapNumberOfChildren extends Mapper<Text, Text, Text, Text> {
}
// NOC equals the number of immediate child classes derived from a base class.
// In Visual Basic .NET one uses the Inherits statement to derive sub-classes.
// In classic Visual Basic inheritance is not available and thus NOC is always
// zero.
//
// NOC measures the breadth of a class hierarchy, where maximum DIT measures the
// depth. Depth is generally better than breadth, since it promotes reuse of
// methods through inheritance. NOC and DIT are closely related. Inheritance
// levels can be added to increase the depth and reduce the breadth.
//
// A high NOC, a large number of child classes, can indicate several things:
//
// High reuse of base class. Inheritance is a form of reuse.
// Base class may require more testing.
// Improper abstraction of the parent class.
// Misuse of sub-classing. In such a case, it may be necessary to group related
// classes and introduce another level of inheritance.
//
// High NOC has been found to indicate fewer faults. This may be due to high
// reuse, which is desirable.
//
// A class with a high NOC and a high WMC indicates complexity at the top of the
// class hierarchy. The class is potentially influencing a large number of
// descendant classes. This can be a sign of poor design. A redesign may be
// required.
//
// Not all classes should have the same number of sub-classes. Classes higher up
// in the hierarchy should have more sub-classes then those lower down.