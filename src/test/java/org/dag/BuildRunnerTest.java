package org.dag;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

class BuildRunnerTest {

    @Test void runs_build_with_enough_workers() {
        Map<String, Integer> buildTimes = Map.of(
                "A", 5,
                "B", 10,
                "C", 8,
                "D", 5);

        //projects along with their dependencies (prerequisites)
        Map<String, Collection<String>> projects = Map.of(
                "A", asList("B", "C"),
                "B", asList("D"),
                "C", asList("D"),
                "D", asList());

        //when
        BuildRunner.BuildResult result = new BuildRunner().build(buildTimes, projects, 5);

        //then
        assertEquals("[D:5, C:8, B:10, A:5]", result.buildOrder.toString());
        //TODO improve instrumentation and add assertions for the total build duration
    }

    @Test void runs_build_with_one_worker() {
        Map<String, Integer> buildTimes = Map.of("A", 5, "B", 10, "C", 15);

        //projects along with their dependencies (prerequisites)
        Map<String, Collection<String>> projects = Map.of("A", asList("B", "C"), "B", asList(), "C", asList());

        //when
        BuildRunner.BuildResult result = new BuildRunner().build(buildTimes, projects, 1);

        //then
        assertEquals("[B:10, C:15, A:5]", result.buildOrder.toString());
    }

    @Test @Disabled("TODO")
    void reports_cycles() {
        Map<String, Integer> buildTimes = Map.of("A", 5, "B", 10, "C", 15);

        Map<String, Collection<String>> projects = Map.of("A", asList("B"), "B", asList("A"), "C", asList());

        //expect
        assertThatThrownBy(() -> new BuildRunner().build(buildTimes, projects, 5))
                .hasMessage("The project-dependencies mapping contains a cycle between nodes: [A, B]");
    }
}