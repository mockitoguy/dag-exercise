package org.dag;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Simplistic build runner that solves the problem of concurrent build in topological order, given limited workers.
 */
class BuildRunner {

    /**
     *  Performs a simulated build for a given project-dependencies mapping, building concurrently, in a topological order.
     *  It's a simplistic algorithm.
     *  It's optimized for showing the first iteration working, with minimum complexity.
     *  I attempted to avoid 'sleeps', minimize shared state, and use built-in Java features.
     *  It's mostly procedural code so that the algorithm can be inspected in a single method.
     *
     *  Pending/future: error handling, sanity timeouts, clean behavior for thread interruption,
     *  clean error messages, detection of dependency cycle,
     *  domain-driven class modelling (instead of procedural code).
     *
     * @param buildTimes mapping of project-buildTime.
     *                   Helps optimizing the algorithm, workers prefer projects with shorter build times.
     * @param projects mapping of project-dependencies.
     *                 Expected to be DAG (no cycles).
     *                 The algorithm runs builds in topological order.
     * @param workers number of concurrent workers.
     * @return build result object offering insights into what happened during the build.
     */
    BuildResult build(Map<String, Integer> buildTimes, Map<String, Collection<String>> projects, int workers) {
        //create a copy of project-dependencies mapping so that we can mutate it, using HashSet for values for O(1) removals.
        projects = projects.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, (e) -> new HashSet<>(e.getValue())));

        //Minimum state shared by threads: 'ready' and 'completed' queues:
        PriorityBlockingQueue<String> completed = new PriorityBlockingQueue<>();
        PriorityBlockingQueue<Buildable> ready = new PriorityBlockingQueue<>();
        //'ready' queue is ordered by buildTime via 'Buildable.compareTo',
        //  this is a simplistic optimization making faster projects preferred by the workers.

        SimpleExecutor executor = new SimpleExecutor(workers);
        executor.onShutdown(() -> ready.add(Buildable.STOP)); //explained later
        executor.submit(() -> {
            while (true) {
                try {
                    // grab the next buildable from the queue (blocking)
                    Buildable buildable = ready.take(); //simplistic, no sanity timeout
                    if (buildable == Buildable.STOP) {
                        //simplistic termination mechanism
                        break;
                    }

                    /* pretending we're building the project... */

                    // adding to the 'completed' queue so that the main thread may schedule more builds
                    completed.add(buildable.project);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e); //simplistic
                }
            }
        });

        BuildResult result = new BuildResult();
        while (!projects.isEmpty()) {
            //collect projects with no dependencies
            List<Buildable> buildableProjects = projects.entrySet().stream()
                    .filter((e) -> e.getValue().isEmpty())
                    .map((e) -> new Buildable(e.getKey(), buildTimes.get(e.getKey())))
                    .collect(Collectors.toList());

            //populate build result information to improve testability (only this thread mutates 'result').
            result.addBuildableProjects(buildableProjects);
            for (Buildable buildable : buildableProjects) {
                //send buildable to the 'ready' queue for building
                ready.add(buildable);
                //remove project so that we won't build any more (only this thread mutates 'projects').
                projects.remove(buildable.project);
            }

            String completedProject;
            try {
                // grab the next completed project from the 'completed' queue (blocking)
                completedProject = completed.take(); //simplistic, no sanity timeout
            } catch (InterruptedException e) {
                throw new RuntimeException(e); //simplistic
            }

            //Remove dependency from every project so that we can build more!
            for (Collection<String> dependencies : projects.values()) {
                dependencies.remove(completedProject); //only this thread mutates 'projects'
                // Alternatively, we can keep project->consumers mapping to avoid iterating the entire map.
            }
        }

        executor.shutdown();

        return result;
    }

    //Thin wrapper over Java's ExecutorService, hiding details less relevant to the algorithm.
    static class SimpleExecutor {
        private final ExecutorService executor;
        private final int workers;
        private Runnable onShutdown;

        SimpleExecutor(int workers) {
            executor = Executors.newFixedThreadPool(workers);
            this.workers = workers;
        }

        void submit(Runnable r) {
            for (int i = 0; i < workers; i++) {
                executor.submit(r);
            }
        }

        void shutdown() {
            for (int i = 0; i < workers; i++) {
                //send stop instruction to every worker, ensuring every task completes
                onShutdown.run();
            }
            executor.shutdown();
            try {
                executor.awaitTermination(10, TimeUnit.SECONDS); //sanity timeout
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        //will be sent to every worker on shutdown
        void onShutdown(Runnable workerStopInstruction) {
            this.onShutdown = workerStopInstruction;
        }
    }

    //Encapsulates sorting of buildables
    static class Buildable implements Comparable<Buildable> {
        private final static Buildable STOP = new Buildable("", 0);
        private final String project;
        private final Integer buildTime;

        Buildable(String project, Integer buildTime) {
            this.project = project;
            this.buildTime = buildTime;
        }

        public int compareTo(Buildable o) {
            return this.buildTime.compareTo(o.buildTime);
        }

        @Override
        public String toString() {
            return project + ':' + buildTime;
        }
    }

    //Instrumentation for testing
    static class BuildResult {
        final List<Buildable> buildOrder = new LinkedList<>();

        void addBuildableProjects(List<Buildable> buildableProjects) {
            buildOrder.addAll(new TreeSet<>(buildableProjects)); //preserving order using TreeSet
        }
    }
}
