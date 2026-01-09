package com.assignment3;

import java.util.UUID;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

public class EmrRunner {

    public static void main(String[] args) {

        String bucketName = "mostafa-ass3-bucket";
        String runId = UUID.randomUUID().toString().substring(0, 5);
        String outputDir = "s3://" + bucketName + "/output/Run_" + runId + "_" + System.currentTimeMillis();

        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3://" + bucketName + "/Jars/dirt-extraction-1.0-SNAPSHOT-jar-with-dependencies.jar")
                .withMainClass("com.assignment3.DirtJob")
                .withArgs("s3://dsp-ass3-first10-biarcs/", "s3://" + bucketName + "/test-set/", outputDir);

        StepConfig stepConfig = new StepConfig()
                .withName("DIRT_Run_" + runId)
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(3)
                .withMasterInstanceType("m5.xlarge")
                .withSlaveInstanceType("m5.xlarge")
                .withEc2KeyName("vockey")
                .withKeepJobFlowAliveWhenNoSteps(false);

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("DIRT Final Run " + runId)
                .withInstances(instances)
                .withSteps(stepConfig)
                .withLogUri("s3://" + bucketName + "/logs/")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-6.13.0");

        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(new ProfileCredentialsProvider())
                .build();

        RunJobFlowResult result = mapReduce.runJobFlow(runFlowRequest);
        System.out.println("Job submitted! ID: " + result.getJobFlowId());
        System.out.println("Wait for completion and check S3: " + outputDir);
    }
}
