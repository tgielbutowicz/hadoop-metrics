package mapper;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.ImportDeclaration;
import com.github.javaparser.ast.NodeList;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.body.FieldDeclaration;
import com.github.javaparser.ast.body.TypeDeclaration;
import com.github.javaparser.ast.expr.BinaryExpr;
import com.github.javaparser.ast.expr.Expression;
import com.github.javaparser.ast.expr.MethodCallExpr;
import com.github.javaparser.ast.type.ClassOrInterfaceType;
import com.github.javaparser.ast.visitor.VoidVisitorAdapter;
import com.google.common.collect.Sets;
import counters.MetricsCounter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Metric;
import utils.MetricsWritable;
import utils.VertexWritable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileMapper extends Mapper<Text, Text, MetricsWritable, VertexWritable> {

    private MetricsWritable key;
    private Set<String> methodsAndCalls;

    private final Pattern methodCallPattern = Pattern.compile("(\\.[\\s\\n\\r]*[\\w]+)[\\s\\n\\r]*(?=\\(.*\\))");
    private final Pattern methodDeclarationPattern = Pattern
            .compile("(public|protected|private|static|\\s) +[\\w\\<\\>\\[\\]]+\\s+(\\w+) *\\([^\\)]*\\) *(\\{?|[^;])");
    private final Logger logger = LoggerFactory.getLogger(RepositoryMapper.class);
    private long startMillis;
    private long endMillis;

    public void map(Text fileNameKey, Text fileValue, Context context) throws IOException, InterruptedException {
        startMillis = System.currentTimeMillis();
        context.getCounter(MetricsCounter.PARSED).increment(1L);
        String fileContents = fileValue.toString().trim();

        methodsAndCalls = Sets.newHashSet();
        try {
            CompilationUnit compilationUnit = JavaParser.parse(fileContents);
            NodeList<ImportDeclaration> imports = compilationUnit.getImports();
            NodeList<TypeDeclaration<?>> types = compilationUnit.getTypes();

            String packageName = getPackageName(compilationUnit);

            Matcher methodDeclarationMatcher = methodDeclarationPattern.matcher(fileContents);
            Matcher methodCallMatcher = methodCallPattern.matcher(fileContents);


            for (TypeDeclaration<?> type : types) {
                String fileName = type.getNameAsString();
                key = new MetricsWritable(new Text(packageName + fileName));

                key.setMetric(Metric.NOC);
                context.write(key, getValueoutAnonymousVertexWithValue(0)); //just to assure that every class have a pair for NOC

                key.setMetric(Metric.WMC);
                while (methodDeclarationMatcher.find()) {
                    methodsAndCalls.add(methodDeclarationMatcher.group());
                }
                context.write(key, getValueoutAnonymousVertexWithValue(methodsAndCalls.size()));

                key.setMetric(Metric.RFC);
                while (methodCallMatcher.find()) {
                    methodsAndCalls.add(methodCallMatcher.group().replace(".", ""));
                }
                context.write(key, getValueoutAnonymousVertexWithValue(methodsAndCalls.size()));

                key.setMetric(Metric.CBO);
                context.write(key, getValueoutAnonymousVertexWithValue(imports.size()));
                int LCOM = 0;
                if (type instanceof ClassOrInterfaceDeclaration) {
                    key.setMetric(Metric.LOC);
                    int lines = type.getEnd().get().line - type.getBegin().get().line;
                    context.write(key, getValueoutAnonymousVertexWithValue(lines));
                    int numberOfFields = 0;
                    int numberOfMethods = type.getMethods().size();
                    double sumMF = 0l;
                    for (FieldDeclaration field : type.getFields()) {
                        numberOfFields++;
                        sumMF += type.getMethods().stream().map(method -> method.getBody().toString()).filter(body -> body.contains(field.toString())).count();
                    }
                    LCOM = Math.toIntExact(Math.round((1 - (sumMF / numberOfMethods * numberOfFields)) * 100));
                }
                key.setMetric(Metric.LCOM);
                context.write(key, getValueoutAnonymousVertexWithValue(LCOM));
            }
            methodsAndCalls.clear();
            new FileMapper.MethodVisitor().visit(compilationUnit, null);

            String clsName;
            String superclsName;
            VertexWritable valueout;
            for (TypeDeclaration<?> type : types) {
                if (type instanceof ClassOrInterfaceDeclaration) {
                    ClassOrInterfaceDeclaration cls = (ClassOrInterfaceDeclaration) type;
                    clsName = packageName + cls.getNameAsString();
                    key = new MetricsWritable(new Text(clsName));
                    if (cls.getExtendedTypes().isEmpty()) {
                        superclsName = "Object";
                        valueout = new VertexWritable();
                        valueout.addVertex(new Text(superclsName));
                        key.setMetric(Metric.DIT);
                        context.write(key, valueout);
                    } else {
                        for (ClassOrInterfaceType supercls : cls.getExtendedTypes()) {
                            superclsName = getFullyQualifiedClassName(supercls, packageName, imports);
                            valueout = new VertexWritable();
                            valueout.addVertex(new Text(superclsName));
                            key.setMetric(Metric.CBO);
                            context.write(key, getValueoutAnonymousVertexWithValue(1));
                            key.setMetric(Metric.DIT);
                            context.write(key, valueout);
                            key.setClassName(superclsName);
                            VertexWritable message = new VertexWritable(new Text(clsName));
                            context.write(key, message);
                            key.setMetric(Metric.NOC);
                            context.write(key, getValueoutAnonymousVertexWithValue(1));
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Parsing error", e);
        }
        endMillis = System.currentTimeMillis();
        context.getCounter(MetricsCounter.DURATION).increment(endMillis - startMillis);
        context.getCounter("File Mapping Time", String.valueOf(this.hashCode())).increment(endMillis - startMillis);
    }

    private String getFullyQualifiedClassName(ClassOrInterfaceType supercls, String packageName, NodeList<ImportDeclaration> imports) {
        Optional<ImportDeclaration> importDeclaration = imports.stream().filter(imp -> imp.getNameAsString().contains(supercls.getNameAsString())).findFirst();
        if (importDeclaration.isPresent()) {
            return importDeclaration.get().getNameAsString();
        } else if (supercls.getScope().isPresent()) {
            return supercls.getScope().get().asString() + "." + supercls.getName().asString();
        } else {
            return packageName + supercls.getName().asString();
        }
    }

    private String getPackageName(CompilationUnit compilationUnit) {
        if (compilationUnit.getPackageDeclaration().isPresent()) {
            return compilationUnit.getPackageDeclaration().toString().replace(";", ".").split("\\s")[1];
        }
        return "nopackage.";
    }

    private VertexWritable getValueoutAnonymousVertexWithValue(int lines) {
        VertexWritable vertexWritable = new VertexWritable(new Text(""));
        vertexWritable.setValue(new IntWritable(lines));
        return vertexWritable;
    }

    private class MethodVisitor extends VoidVisitorAdapter {
        @Override
        public void visit(MethodCallExpr methodCall, Object arg) {
            methodsAndCalls.add(methodCall.getNameAsString());
            List<Expression> args = methodCall.getArguments();
            if (args != null)
                handleExpressions(args);
        }

        private void handleExpressions(List<Expression> expressions) {
            for (Expression expr : expressions) {
                if (expr instanceof MethodCallExpr)
                    visit((MethodCallExpr) expr, null);
                else if (expr instanceof BinaryExpr) {
                    BinaryExpr binExpr = (BinaryExpr) expr;
                    handleExpressions(Arrays.asList(binExpr.getLeft(), binExpr.getRight()));
                }
            }
        }
    }
}
