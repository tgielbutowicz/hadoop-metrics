package mapper;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class RepositoryDownloadMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Logger logger = LoggerFactory.getLogger(RepositoryDownloadMapper.class);
    private static final String JAVA = ".java";

    public void map(LongWritable key, Text repoURL, Context context) throws IOException {
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
                logger.debug("Repository exists. Updating: {}", destination.getPath());
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
                        context.write(new Text(key.toString()), new Text(loader.getBytes()));
                    } catch (Exception e) {
                        logger.error("Error writing class file {}", e);
                    }
                }
            } finally {
                index.unlock();
            }
            repository.close();
        } catch (GitAPIException gite) {
            logger.error("Error while cloning repository {}", gite);
        }
    }
}
