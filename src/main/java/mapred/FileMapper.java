package mapred;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.ImportDeclaration;
import com.github.javaparser.ast.NodeList;
import com.github.javaparser.ast.PackageDeclaration;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.body.FieldDeclaration;
import com.github.javaparser.ast.body.TypeDeclaration;
import com.github.javaparser.ast.expr.BinaryExpr;
import com.github.javaparser.ast.expr.Expression;
import com.github.javaparser.ast.expr.MethodCallExpr;
import com.github.javaparser.ast.type.ClassOrInterfaceType;
import com.github.javaparser.ast.visitor.VoidVisitorAdapter;
import com.google.common.collect.Sets;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
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

    public void map(Text keyPath, Text value, Context context) throws IOException, InterruptedException {
        methodsAndCalls = Sets.newHashSet();
        String fileContents = value.toString().trim();
        CompilationUnit compilationUnit = JavaParser.parse(fileContents);
        NodeList<ImportDeclaration> imports = compilationUnit.getImports();
        NodeList<TypeDeclaration<?>> types = compilationUnit.getTypes();

        Optional<PackageDeclaration> packageDeclaration = compilationUnit.getPackageDeclaration();
        String packageName = "nopackage";
        if (packageDeclaration.isPresent()) {
            packageName = packageDeclaration.toString().replace(";", ".").split("\\s")[1];
        }
        Pattern tagPattern = Pattern
                .compile("(public|protected|private|static|\\s) +[\\w\\<\\>\\[\\]]+\\s+(\\w+) *\\([^\\)]*\\) *(\\{?|[^;])");
        Matcher tagMatcher = tagPattern.matcher(fileContents);
        Pattern methodCallPattern = Pattern.compile("(\\.[\\s\\n\\r]*[\\w]+)[\\s\\n\\r]*(?=\\(.*\\))");
        Matcher methodCallMatcher = methodCallPattern.matcher(fileContents);

        key = new MetricsWritable(new Text(), new Text(packageName + keyPath));
        key.setMetric(Metric.NOC);
        context.write(key, getValueoutAnonymousVertexWithValue(0)); //just to assure that every class have a pair for NOC

        key.setMetric(Metric.WMC);
        int methods = 0;
        while (tagMatcher.find()) {
            methodsAndCalls.add(tagMatcher.group());
            methods++;
        }
        context.write(key, getValueoutAnonymousVertexWithValue(methods));

        key.setMetric(Metric.RFC);
        while (methodCallMatcher.find()) {
            methodsAndCalls.add(methodCallMatcher.group().replace(".", ""));
        }
        context.write(key, getValueoutAnonymousVertexWithValue(methodsAndCalls.size()));

        key.setMetric(Metric.CBO);
        context.write(key, getValueoutAnonymousVertexWithValue(imports.size()));

        for (TypeDeclaration<?> type : types) {
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
        new MethodVisitor().visit(compilationUnit, null);

        String clsName;
        String superclsName;
        VertexWritable valueout;
        for (TypeDeclaration<?> type : types) {
            if (type instanceof ClassOrInterfaceDeclaration) {
                ClassOrInterfaceDeclaration cls = (ClassOrInterfaceDeclaration) type;
                clsName = packageName + cls.getNameAsString();
                if (cls.getExtendedTypes().isEmpty()) {
                    superclsName = "Object";
                    valueout = new VertexWritable();
                    valueout.addVertex(new Text(superclsName));
                    key.setMetric(Metric.DIT);
                    context.write(key, valueout);
                } else {
                    for (ClassOrInterfaceType supercls : cls.getExtendedTypes()) {
                        Optional<ImportDeclaration> importDeclaration = imports.stream().filter(imp -> imp.getNameAsString().contains(supercls.getNameAsString())).findFirst();
                        if (importDeclaration.isPresent()) {
                            superclsName = importDeclaration.get().getNameAsString();
                        } else {
                            superclsName = packageName + supercls.getName().asString();
                        }
                        valueout = new VertexWritable();
                        valueout.addVertex(new Text(superclsName));
                        key.setMetric(Metric.CBO);
                        context.write(key, getValueoutAnonymousVertexWithValue(1));
                        key.setMetric(Metric.DIT);
                        context.write(key, valueout);
                        key.setFile(superclsName);
                        VertexWritable message = new VertexWritable(new Text(clsName));
                        context.write(key, message);
                        key.setMetric(Metric.NOC);
                        context.write(key, getValueoutAnonymousVertexWithValue(1));
                    }
                }
            }
        }
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
