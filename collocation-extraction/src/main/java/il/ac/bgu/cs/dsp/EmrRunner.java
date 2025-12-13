package il.ac.bgu.cs.dsp;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

public class EmrRunner {

    public static void main(String[] args) {
        
        // --- CONFIGURATION ---
        String bucketName = "mosta-s3-bucket"; 
        String keyPairName = "vockey";      
        String logUri = "s3://" + bucketName + "/logs/";
        String jarUrl = "s3://" + bucketName + "/jars/assignment2-1.0-SNAPSHOT.jar";
        
        // Google N-Grams S3 Paths (English) [cite: 24, 27]
        String input1Gram = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/1gram/data";
        String input2Gram = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data";
        String outputDir = "s3://" + bucketName + "/output/" + System.currentTimeMillis(); // Unique output folder
        String language = "eng";

        // --- STEP CONFIGURATION ---
        // We run the ExtractCollocations class (which runs Job 1 -> 2 -> 3 -> 4)
        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar(jarUrl) // Amazon EMR downloads this JAR from S3
                .withMainClass("il.ac.bgu.cs.dsp.ExtractCollocations")
                .withArgs(input1Gram, input2Gram, outputDir, language);

        StepConfig stepConfig = new StepConfig()
                .withName("ExtractCollocations")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // --- CLUSTER INSTANCES ---
        // Use M4.large as recommended [cite: 108]
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(3) // 1 Master + 2 Slaves
                .withMasterInstanceType("m4.large")
                .withSlaveInstanceType("m4.large")
                .withEc2KeyName(keyPairName)
                .withKeepJobFlowAliveWhenNoSteps(false); // Auto-terminate to save money [cite: 112]
            

        // --- RUN REQUEST ---
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Collocation Extraction Job")
                .withInstances(instances)
                .withSteps(stepConfig)
                .withLogUri(logUri)
                .withServiceRole("EMR_DefaultRole") 
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.29.0"); // Matches Hadoop 2.10.x

        // --- EXECUTE ---
        // Credentials are loaded from ~/.aws/credentials (run 'aws configure' in terminal first)
        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(new ProfileCredentialsProvider()) 
                .build();

        RunJobFlowResult result = mapReduce.runJobFlow(runFlowRequest);
        System.out.println("Job submitted! ID: " + result.getJobFlowId());
        System.out.println("Check status in AWS Console EMR: https://console.aws.amazon.com/elasticmapreduce/");
    }
}