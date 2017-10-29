package mapred.cbo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapCouplingBetweenObjectClasses extends Mapper<Text, Text, Text, Text> {
}
// Two classes are coupled when methods declared in one class use methods or
// instance variables defined by the other class. The uses relationship can go
// either way: both uses and used-by relationships are taken into account, but
// only once.
//
// Multiple accesses to the same class are counted as one access. Only method
// calls and variable references are counted. Other types of reference, such as
// use of constants, calls to API declares, handling of events, use of
// user-defined types, and object instantiations are ignored. If a method call
// is polymorphic (either because of Overrides or Overloads), all the classes to
// which the call can go are included in the coupled count.
//
// High CBO is undesirable. Excessive coupling between object classes is
// detrimental to modular design and prevents reuse. The more independent a
// class is, the easier it is to reuse it in another application. In order to
// improve modularity and promote encapsulation, inter-object class couples
// should be kept to a minimum. The larger the number of couples, the higher the
// sensitivity to changes in other parts of the design, and therefore
// maintenance is more difficult. A high coupling has been found to indicate
// fault-proneness. Rigorous testing is thus needed. — How high is too high?
// CBO>14 is too high, say Sahraoui, Godin & Miceli in their article (link
// below).
//
// A useful insight into the 'object-orientedness' of the design can be gained
// from the system wide distribution of the class fan-out values. For example a
// system in which a single class has very high fan-out and all other classes
// have low or zero fan-outs, we really have a structured, not an object
// oriented, system.
//
// Implementation details. The definition of CBO deals with the instance
// variables and all the methods of a class. In VB.NET terms, this means
// non-Shared variables and Shared & non-Shared methods. Thus, Shared variables
// (class variables) are not taken into account. On the contrary, all method
// calls are taken into account, whether Shared or not. This distinction does
// not seem to make any sense, but we follow the original definition.
//
// If a call is polymorphic in that it is to an Interface method in .NET, this
// is not taken as a coupling to either the Interface or the classes that
// implement the interface. If a call is polymorphic in that it is to a method
// defined in a VB Classic interface class (base class), it's a coupling to the
// interface class, but not to any classes that implement the interface. This is
// a limitation of the implementation, not the definition of CBO.
//
// In this implementation of CBO, when a child class calls its own inherited
// methods, it is coupled to the parent class where the methods are defined. The
// original CBO definition does not define if inheritance should be treated in
// any specific way. Therefore, we follow the definition and treat inheritance
// as if it was regular coupling.
