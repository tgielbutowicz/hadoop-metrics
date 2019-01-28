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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import counters.MapperCounter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.dircache.DirCache;
import org.eclipse.jgit.dircache.DirCacheIterator;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.RepositoryCache;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.treewalk.filter.PathSuffixFilter;
import org.eclipse.jgit.util.FS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Metric;
import utils.MetricsWritable;
import utils.VertexWritable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RepositoryMapper extends Mapper<LongWritable, Text, MetricsWritable, VertexWritable> {

    private static final String JAVA = ".java";
    private final Pattern methodCallPattern = Pattern.compile("(\\.[\\s\\n\\r]*[\\w]+)[\\s\\n\\r]*(?=\\(.*\\))");
    private final Pattern methodDeclarationPattern = Pattern
            .compile("(public|protected|private|static|\\s) +[\\w\\<\\>\\[\\]]+\\s+(\\w+) *\\([^\\)]*\\) *(\\{?|[^;])");
    private final Logger logger = LoggerFactory.getLogger(RepositoryMapper.class);
    private MetricsWritable key;
    private Set<String> methodsAndCalls;
    private long startMillis;
    private long endMillis;

    public void map(LongWritable key, Text repoURL, Context context) throws IOException {
        startMillis = System.currentTimeMillis();
        String project = Iterables.getLast(Lists.newArrayList(repoURL.toString().split("/"))).replace(".git", "");
        File destination = new File(context.getConfiguration().get("working.path") + "/" + project + "/");
        try {
            Repository repository;
            if (!Files.exists(destination.toPath()) && !RepositoryCache.FileKey.isGitRepository(destination, FS.DETECTED)) {
                logger.debug("Cloning repository from {} to {}", repoURL.toString(), destination.getPath());
                repository = Git.cloneRepository()
                        .setURI(repoURL.toString())
                        .setDirectory(destination)
                        .call().getRepository();
            } else {
                logger.debug("Repository exists. Opening: {}", destination.getPath());
                Git.open(destination).pull().call();
                repository = Git.open(destination).getRepository();
            }
            DirCache index = repository.lockDirCache();
            try (TreeWalk treeWalk = new TreeWalk(repository)) {
                treeWalk.addTree(new DirCacheIterator(index));
                treeWalk.setRecursive(true);
                treeWalk.setFilter(PathSuffixFilter.create(JAVA));
                while (treeWalk.next()) {
                    ObjectId fileId = treeWalk.getObjectId(0);
                    ObjectLoader loader = repository.open(fileId);
                    try {
                        emitMessages(treeWalk.getNameString(), new String(loader.getBytes()), context);
                    } catch (Exception e) {
                        logger.error("Metrix caculation error {}", e);
                    }
                }
            } finally {
                index.unlock();
            }
            repository.close();
        } catch (GitAPIException gite) {
            logger.error("Error while cloning repository {}", gite);
        }
        endMillis = System.currentTimeMillis();
        context.getCounter(MapperCounter.DURATION).increment(endMillis - startMillis);
        context.getCounter("Repository Mapping Time",String.valueOf(this.hashCode())).increment(endMillis - startMillis);
    }

    private void emitMessages(String fileName, String fileContents, Context context) throws IOException, InterruptedException {
        methodsAndCalls = Sets.newHashSet();
        CompilationUnit compilationUnit = JavaParser.parse(fileContents);
        NodeList<ImportDeclaration> imports = compilationUnit.getImports();
        NodeList<TypeDeclaration<?>> types = compilationUnit.getTypes();

        String packageName = getPackageName(compilationUnit);

        Matcher methodDeclarationMatcher = methodDeclarationPattern.matcher(fileContents);
        Matcher methodCallMatcher = methodCallPattern.matcher(fileContents);
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
                    logger.debug("Object inheritance {}", cls.getName());
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
        return "nopackage";
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
