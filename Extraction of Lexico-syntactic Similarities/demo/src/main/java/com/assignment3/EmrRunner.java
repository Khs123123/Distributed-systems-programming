package com.assignment3;

import java.util.UUID;

import com.amazonaws.AmazonServiceException;
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
        // שם הבאקט שלך
        String bucketName = "mostafa-ass3-bucket";
        String runId = UUID.randomUUID().toString().substring(0, 5);
        
        // שינוי ל-s3:// (ה-API של EMR מחייב זאת)
        String outputDir = "s3://" + bucketName + "/output/Run_Large_Scale_" + runId;
        String inputPath = "s3://" + bucketName + "/input/biarcs-large/";
        String testSetPath = "s3://" + bucketName + "/test-set/full_test_set.txt";
        String jarPath = "s3://" + bucketName + "/Jars/dirt-extraction-1.0-SNAPSHOT-jar-with-dependencies.jar";
        String logUri = "s3://" + bucketName + "/logs/";

        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar(jarPath)
                .withMainClass("com.assignment3.DirtJob")
                .withArgs(inputPath, testSetPath, outputDir);

        StepConfig stepConfig = new StepConfig()
                .withName("DIRT_LARGE_Run_" + runId)
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(9)
                .withMasterInstanceType("m5.xlarge")
                .withSlaveInstanceType("m5.xlarge")
                .withEc2KeyName("vockey") // וודא שזה השם ב-AWS Console -> EC2 -> Key Pairs
                .withKeepJobFlowAliveWhenNoSteps(false);

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("DIRT Final Large Run " + runId)
                .withInstances(instances)
                .withSteps(stepConfig)
                .withLogUri(logUri)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-6.13.0");

        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(new ProfileCredentialsProvider())
                .build();

        System.out.println("Submitting Job Flow...");

        try {
            RunJobFlowResult result = mapReduce.runJobFlow(runFlowRequest);
            System.out.println("Large-scale Job submitted! ID: " + result.getJobFlowId());
        } catch (AmazonServiceException e) {
            System.err.println("--- AWS Validation Error ---");
            System.err.println("Message: " + e.getErrorMessage());
            System.err.println("Code: " + e.getErrorCode());
            System.err.println("Type: " + e.getErrorType());
        }
    }
}