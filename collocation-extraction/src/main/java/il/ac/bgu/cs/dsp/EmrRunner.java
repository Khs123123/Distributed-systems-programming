package il.ac.bgu.cs.dsp;

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
        
        // CONFIGURATION 
        String bucketName = "mosta-s3-bucket"; 
        String keyPairName = "vockey";      
        String logUri = "s3://" + bucketName + "/logs/";
        String jarUrl = "s3://" + bucketName + "/jars/assignment2-1.0-SNAPSHOT.jar";
        
        // BRITISH ENGLISH DATASET 
        //String input1Gram = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/1gram/data";
        //String input2Gram = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/2gram/data";

        // String language = "eng"; 

        // Unique output folder
        //String outputDir = "s3://" + bucketName + "/output/TEST_ENGLISH_With Local Aggregation" + System.currentTimeMillis();

        String input1Gram = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data";
        String input2Gram = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";

        String language = "heb";

        String outputDir = "s3://" + bucketName + "/output/TEST_HEBREW_With Local Aggregation" + System.currentTimeMillis(); 

        

        // STEP CONFIGURATION 
        // Configures the Hadoop Job to run the ExtractCollocations driver
        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar(jarUrl) 
                .withMainClass("il.ac.bgu.cs.dsp.ExtractCollocations")
                .withArgs(input1Gram, input2Gram, outputDir, language);

        StepConfig stepConfig = new StepConfig()
                .withName("ExtractCollocations")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(6) // 1 Master + 5 Slaves
                .withMasterInstanceType("m5.xlarge")
                .withSlaveInstanceType("m5.xlarge")
                .withEc2KeyName(keyPairName)
                .withKeepJobFlowAliveWhenNoSteps(false);
            

        // --- Job Submission ---
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Collocation Extraction Job")
                .withInstances(instances)
                .withSteps(stepConfig)
                .withLogUri(logUri)
                .withServiceRole("EMR_DefaultRole") 
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.29.0");

        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(new ProfileCredentialsProvider()) 
                .build();

        RunJobFlowResult result = mapReduce.runJobFlow(runFlowRequest);
        System.out.println("Job submitted! ID: " + result.getJobFlowId());
        System.out.println("Check status in AWS Console EMR: https://console.aws.amazon.com/elasticmapreduce/");
    }
}