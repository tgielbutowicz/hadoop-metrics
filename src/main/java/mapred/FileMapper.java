package mapred;

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
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileMapper extends Mapper<MetricsWritable, Text, MetricsWritable, VertexWritable> {

    private String cls;
    private String supercls;
    private Set<String> methodsAndCalls = Sets.newHashSet();

    public void map(MetricsWritable key, Text value, Context context) throws IOException, InterruptedException {

        String fileContents = value.toString().trim();
        CompilationUnit compilationUnit = JavaParser.parse(fileContents);
        NodeList<ImportDeclaration> imports = compilationUnit.getImports();
        NodeList<TypeDeclaration<?>> types = compilationUnit.getTypes();

        Pattern newLinePattern = Pattern.compile("\r\n|\r|\n");
        Pattern tagPattern = Pattern
                .compile("(public|protected|private|static|\\s) +[\\w\\<\\>\\[\\]]+\\s+(\\w+) *\\([^\\)]*\\) *(\\{?|[^;])");
        Matcher locMatcher = newLinePattern.matcher(fileContents);
        Matcher tagMatcher = tagPattern.matcher(fileContents);
        Pattern classPattern = Pattern.compile("\\s*(public|private)\\s+class\\s+(\\w+)");
        Pattern superclassPattern = Pattern.compile("\\s+(extends\\s+)+(\\w+)");
        Matcher classMatcher = classPattern.matcher(fileContents);
        Matcher superclassMatcher = superclassPattern.matcher(fileContents);
        Pattern methodCallPattern = Pattern.compile("(\\.[\\s\\n\\r]*[\\w]+)[\\s\\n\\r]*(?=\\(.*\\))");
        Matcher methodCallMatcher = methodCallPattern.matcher(fileContents);

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

        key.setMetric(Metric.LOC);
        int lines = 0;
        while (locMatcher.find()) {
            lines++;
        }
        context.write(key, getValueoutAnonymousVertexWithValue(lines));

        key.setMetric(Metric.CBO);
        context.write(key, getValueoutAnonymousVertexWithValue(imports.size()));

        key.setMetric(Metric.LCOM);
        for (TypeDeclaration<?> type : types) {
            int LCOM = 0;
            if (type instanceof ClassOrInterfaceDeclaration) {
                int numberOfFields = 0;
                int numberOfMethods = type.getMethods().size();
                float sumMF = 0l;
                for (FieldDeclaration field : type.getFields()) {
                    numberOfFields++;
                    sumMF += type.getMethods().stream().map(method -> method.getBody().toString()).filter(body -> body.contains(field.toString())).count();
                }
                LCOM = Math.round((1 - (sumMF/numberOfMethods*numberOfFields)) * 100);
            }
            context.write(key, getValueoutAnonymousVertexWithValue(LCOM));
        }
        methodsAndCalls.clear();
        new MethodVisitor().visit(compilationUnit, null);

        if (classMatcher.find()) {
            key.setMetric(Metric.DIT);
            cls = classMatcher.group(2);
            if (superclassMatcher.find()) {
                supercls = superclassMatcher.group(2) + ".java";
            } else {
                supercls = "Object.java";
            }
            VertexWritable valueout = new VertexWritable();
            valueout.addVertex(new Text(supercls));
            context.write(key, valueout);
            key.setFile(supercls);
            VertexWritable message = new VertexWritable(new Text(cls));
            context.write(key, message);
            key.setMetric(Metric.NOC);
            context.write(key, getValueoutAnonymousVertexWithValue(1));
            key.setMetric(Metric.CBO);
            context.write(key, getValueoutAnonymousVertexWithValue(1));
        } else {
            key.setMetric(Metric.NOC);
            context.write(key, getValueoutAnonymousVertexWithValue(0));
        }

        key.setMetric(Metric.LOC);
        key.setFile("Total LOC");
        context.write(key, getValueoutAnonymousVertexWithValue(lines));
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
