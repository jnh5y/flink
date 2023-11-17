/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.nodes.exec.testutils;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.utils.ExecNodeMetadataUtil;
import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.SqlTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.table.test.program.TableTestProgramRunner;
import org.apache.flink.table.test.program.TestStep.TestKind;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.apache.commons.collections.CollectionUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for implementing restore tests for {@link ExecNode}.You can generate json compiled
 * plan and a savepoint for the latest node version by running {@link
 * RestoreTestBase#generateTestSetupFiles(TableTestProgram)} which is disabled by default.
 *
 * <p><b>Note:</b> The test base uses {@link TableConfigOptions.CatalogPlanCompilation#SCHEMA}
 * because it needs to adjust source and sink properties before and after the restore. Therefore,
 * the test base can not be used for testing storing table options in the compiled plan.
 */
@ExtendWith(MiniClusterExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(org.junit.jupiter.api.MethodOrderer.OrderAnnotation.class)
public abstract class RestoreTestBase implements TableTestProgramRunner {

    private final Class<? extends ExecNode> execNodeUnderTest;

    protected RestoreTestBase(Class<? extends ExecNode> execNodeUnderTest) {
        this.execNodeUnderTest = execNodeUnderTest;
    }

    @Override
    public EnumSet<TestKind> supportedSetupSteps() {
        return EnumSet.of(
                TestKind.FUNCTION,
                TestKind.SOURCE_WITH_RESTORE_DATA,
                TestKind.SINK_WITH_RESTORE_DATA);
    }

    @Override
    public EnumSet<TestKind> supportedRunSteps() {
        return EnumSet.of(TestKind.SQL);
    }

    private @TempDir Path tmpDir;

    private List<ExecNodeMetadata> getAllMetadata() {
        return ExecNodeMetadataUtil.extractMetadataFromAnnotation(execNodeUnderTest);
    }

    private ExecNodeMetadata getLatestMetadata() {
        return ExecNodeMetadataUtil.latestAnnotation(execNodeUnderTest);
    }

    private Stream<Arguments> createSpecs() {
        return getAllMetadata().stream()
                .flatMap(
                        metadata ->
                                supportedPrograms().stream().map(p -> Arguments.of(p, metadata)));
    }

    /**
     * Execute this test to generate test files. Remember to be using the correct branch when
     * generating the test files.
     */
    @Disabled
    @ParameterizedTest
    @MethodSource("supportedPrograms")
    @Order(0)
    public void generateTestSetupFiles(TableTestProgram program) throws Exception {
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.getConfig()
                .set(
                        TableConfigOptions.PLAN_COMPILE_CATALOG_OBJECTS,
                        TableConfigOptions.CatalogPlanCompilation.SCHEMA);
        tEnv.getConfig().set(TableConfigOptions.PLAN_FORCE_RECOMPILE, true);
        for (SourceTestStep sourceTestStep : program.getSetupSourceTestSteps()) {
            final String id = TestValuesTableFactory.registerData(sourceTestStep.dataBeforeRestore);
            final Map<String, String> options = new HashMap<>();
            options.put("connector", "values");
            options.put("data-id", id);
            options.put("terminating", "false");
            options.put("disable-lookup", "true");
            options.put("runtime-source", "NewSource");
            sourceTestStep.apply(tEnv, options);
        }

        final List<CompletableFuture<?>> futures = new ArrayList<>();
        for (SinkTestStep sinkTestStep : program.getSetupSinkTestSteps()) {
            final CompletableFuture<Object> future = new CompletableFuture<>();
            futures.add(future);
            final String tableName = sinkTestStep.name;
            TestValuesTableFactory.registerLocalRawResultsObserver(
                    tableName,
                    (integer, strings) -> {
                        final boolean shouldTakeSavepoint =
                                CollectionUtils.isEqualCollection(
                                        TestValuesTableFactory.getResultsAsStrings(tableName),
                                        sinkTestStep.getExpectedBeforeRestoreAsStrings());
                        System.out.println(
                                "Got results: "
                                        + shouldTakeSavepoint
                                        + " : \n"
                                        + sinkTestStep.getExpectedBeforeRestoreAsStrings()
                                        + "\n"
                                        + TestValuesTableFactory.getRawResultsAsStrings(tableName));
                        if (shouldTakeSavepoint) {
                            // Need to add boolean to show that we have received all the expected
                            // values.
                            // If we receive more, throw an exception.
                            System.out.println("Taking save point!");
                            future.complete(null);
                        }
                    });
            final Map<String, String> options = new HashMap<>();
            options.put("connector", "values");
            options.put("disable-lookup", "true");
            options.put("sink-insert-only", "false");
            sinkTestStep.apply(tEnv, options);
        }

        program.getSetupFunctionTestSteps().forEach(s -> s.apply(tEnv));

        final SqlTestStep sqlTestStep = program.getRunSqlTestStep();

        final CompiledPlan compiledPlan = tEnv.compilePlanSql(sqlTestStep.sql);
        compiledPlan.writeToFile(getPlanPath(program, getLatestMetadata()));

        final TableResult tableResult = compiledPlan.execute();
        CompletableFuture<Void> all =
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        // Wait on all to be completed
        all.get(5, TimeUnit.SECONDS);
        final JobClient jobClient = tableResult.getJobClient().get();
        final String savepoint =
                jobClient
                        .stopWithSavepoint(false, tmpDir.toString(), SavepointFormatType.DEFAULT)
                        .get();
        CommonTestUtils.waitForJobStatus(jobClient, Collections.singletonList(JobStatus.FINISHED));
        final Path savepointPath = Paths.get(new URI(savepoint));
        final Path savepointDirPath = getSavepointPath(program, getLatestMetadata());
        // Delete directory savepointDirPath if it already exists
        if (Files.exists(savepointDirPath)) {
            Files.walk(savepointDirPath).map(Path::toFile).forEach(java.io.File::delete);
        } else {
            Files.createDirectories(savepointDirPath);
        }
        Files.move(savepointPath, savepointDirPath, StandardCopyOption.ATOMIC_MOVE);

        for (SinkTestStep sinkTestStep : program.getSetupSinkTestSteps()) {
            TestValuesTableFactory.unregisterLocalRawResultsObserver(sinkTestStep.name);
        }
    }

    @ParameterizedTest
    @MethodSource("createSpecs")
    @Order(1)
    void testRestore(TableTestProgram program, ExecNodeMetadata metadata) throws Exception {
        final EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        final SavepointRestoreSettings restoreSettings =
                SavepointRestoreSettings.forPath(
                        getSavepointPath(program, metadata).toString(),
                        false,
                        RestoreMode.NO_CLAIM);
        SavepointRestoreSettings.toConfiguration(restoreSettings, settings.getConfiguration());
        final TableEnvironment tEnv = TableEnvironment.create(settings);
        tEnv.getConfig()
                .set(
                        TableConfigOptions.PLAN_RESTORE_CATALOG_OBJECTS,
                        TableConfigOptions.CatalogPlanRestore.IDENTIFIER);

        boolean isTerminatingSource = true;
        for (SourceTestStep sourceTestStep : program.getSetupSourceTestSteps()) {
            final String id = TestValuesTableFactory.registerData(sourceTestStep.dataAfterRestore);
            final Map<String, String> options = new HashMap<>();
            options.put("connector", "values");
            options.put("data-id", id);
            options.put("disable-lookup", "true");
            options.put("runtime-source", "NewSource");
            sourceTestStep.apply(tEnv, options);
            if (sourceTestStep.options.getOrDefault("terminating", "true").equals("false")) {
                isTerminatingSource = false;
            }
        }

        final List<CompletableFuture<?>> futures = new ArrayList<>();

        for (SinkTestStep sinkTestStep : program.getSetupSinkTestSteps()) {
            if (!isTerminatingSource) {
                final CompletableFuture<Object> future = new CompletableFuture<>();
                futures.add(future);
                final String tableName = sinkTestStep.name;
                System.out.println("Registering observer for " + sinkTestStep.name);
                TestValuesTableFactory.registerLocalRawResultsObserver(
                        tableName,
                        (integer, strings) -> {
                            System.out.println(
                                    "Got results: \n"
                                            + sinkTestStep.getExpectedBeforeRestoreAsStrings()
                                            + "\n"
                                            + sinkTestStep.getExpectedAfterRestoreAsStrings()
                                            + "\n"
                                            + TestValuesTableFactory.getRawResultsAsStrings(tableName));
                            List<String> results = new ArrayList<>();
                            results.addAll(sinkTestStep.getExpectedBeforeRestoreAsStrings());
                            results.addAll(sinkTestStep.getExpectedAfterRestoreAsStrings());
                            final boolean shouldComplete =
                                    CollectionUtils.isEqualCollection(
                                            TestValuesTableFactory.getRawResultsAsStrings(
                                                    tableName),
                                            results);
                            if (shouldComplete) {
                                future.complete(null);
                            }
                        });
            }
            final Map<String, String> options = new HashMap<>();
            options.put("connector", "values");
            options.put("disable-lookup", "true");
            options.put("sink-insert-only", "false");
            sinkTestStep.apply(tEnv, options);
        }

        program.getSetupFunctionTestSteps().forEach(s -> s.apply(tEnv));

        final CompiledPlan compiledPlan =
                tEnv.loadPlan(PlanReference.fromFile(getPlanPath(program, metadata)));

        TableResult tableResult = null;
        if (!isTerminatingSource) {
            System.out.println("Waiting for all results for not terminating source");
            tableResult = compiledPlan.execute();
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        } else {
            System.out.println("Waiting for all results for terminating source");
            compiledPlan.execute().await();
        }
        System.out.println("Got here");

        for (SinkTestStep sinkTestStep : program.getSetupSinkTestSteps()) {
            // TODO: Fix this.  If the records after a restore cause retractions,
            // this approach will not work.
            assertThat(TestValuesTableFactory.getResultsAsStrings(sinkTestStep.name))
                    .containsExactlyInAnyOrder(
                            Stream.concat(
                                            sinkTestStep.getExpectedBeforeRestoreAsStrings()
                                                    .stream(),
                                            sinkTestStep.getExpectedAfterRestoreAsStrings()
                                                    .stream())
                                    .toArray(String[]::new));
        }
        for (SinkTestStep sinkTestStep : program.getSetupSinkTestSteps()) {
            System.out.println("Unregistering observer for " + sinkTestStep.name);
            TestValuesTableFactory.unregisterLocalRawResultsObserver(sinkTestStep.name);
        }

        if (!isTerminatingSource) {
            final JobClient jobClient = tableResult.getJobClient().get();
//            final String savepoint =
                    jobClient.cancel().get(5, TimeUnit.SECONDS);
//                            .stopWithSavepoint(false, tmpDir.toString(), SavepointFormatType.DEFAULT)
//                            .get();
        }
    }

    private Path getPlanPath(TableTestProgram program, ExecNodeMetadata metadata) {
        return Paths.get(
                getTestResourceDirectory(program, metadata) + "/plan/" + program.id + ".json");
    }

    private Path getSavepointPath(TableTestProgram program, ExecNodeMetadata metadata) {
        return Paths.get(getTestResourceDirectory(program, metadata) + "/savepoint/");
    }

    private String getTestResourceDirectory(TableTestProgram program, ExecNodeMetadata metadata) {
        return String.format(
                "%s/src/test/resources/restore-tests/%s_%d/%s",
                System.getProperty("user.dir"), metadata.name(), metadata.version(), program.id);
    }
}
