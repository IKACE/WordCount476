package com.sample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
/*
IntWritable is the Hadoop variant of Integer which has been optimized for serialization in the Hadoop environment.
An integer would use the default Java Serialization which is very costly in Hadoop environment.
see: https://stackoverflow.com/questions/52361265/integer-and-intwritable-types-existence
 */

public class TwoMostFrequentStopWords {

    // We have created a class TokenizerMapper that extends
    // the class Mapper which is already defined in the MapReduce Framework.
    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, Text, LongWritable>{

        // Output:
        // We have the hardcoded value in our case which is 1: IntWritable
        private final static IntWritable one = new IntWritable(1);
        // The key is the tokenized words: Text
        private Text word = new Text();

        // Input:
        // We define the data types of input and output key/value pair after the class declaration using angle brackets.
        // Both the input and output of the Mapper is a key/value pair.

        // The key is nothing but the offset of each line in the text file: LongWritable
        // The value is each individual line (as shown in the figure at the right): Text
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, key);
            }
        }
        // We have written a java code where we have tokenized each word
        // and assigned them a hardcoded value equal to 1.
        // Eg: Dear 1, Bear 1,
    }

    public static class IntSumReducer
            extends Reducer<Text,LongWritable,Text,Text> {
        private Text result = new Text();

        // Input:
        // The key nothing but those unique words which have been generated after the sorting and shuffling phase: Text
        // The value is a list of integers corresponding to each key: IntWritable
        // Eg: Bear, [1, 1],
        // Output:
        // The key is all the unique words present in the input text file: Text
        // The value is the number of occurrences of each of the unique words: IntWritable
        // Eg: Bear, 2; Car, 3,

        public void reduce(Text key, Iterable<LongWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            String res_str = new String();
            for (LongWritable val : values) {
                res_str += val.toString();
                res_str += ",";
            }
            result = new Text(res_str);
            context.write(key, result);
        }
    }

    // Mapper and Reducer for counting number of documents
    public static class CounterMapper
            extends Mapper<LongWritable, Text, IntWritable, LongWritable>{

        // Output:
        // We have the hardcoded value in our case which is 1: IntWritable
        private final static IntWritable one = new IntWritable(1);
        // The key is the tokenized words: Text

        // Input:
        // We define the data types of input and output key/value pair after the class declaration using angle brackets.
        // Both the input and output of the Mapper is a key/value pair.

        // The key is nothing but the offset of each line in the text file: LongWritable
        // The value is each individual line (as shown in the figure at the right): Text
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            context.write(one, key);
        }
        // We have written a java code where we have tokenized each word
        // and assigned them a hardcoded value equal to 1.
        // Eg: Dear 1, Bear 1,
    }

    public static class CounterReducer
            extends Reducer<IntWritable,LongWritable,Text,IntWritable> {
        private final static Text output_str = new Text("num_lines");

        // Input:
        // The key nothing but those unique words which have been generated after the sorting and shuffling phase: Text
        // The value is a list of integers corresponding to each key: IntWritable
        // Eg: Bear, [1, 1],
        // Output:
        // The key is all the unique words present in the input text file: Text
        // The value is the number of occurrences of each of the unique words: IntWritable
        // Eg: Bear, 2; Car, 3,

        public void reduce(IntWritable key, Iterable<LongWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int count = 0;
            for (LongWritable value: values) {
                count++;
            }
            IntWritable res = new IntWritable(count);
            context.write(output_str, res);
        }
    }

    // Mapper and reducer for counting how many time a word appear in documents
    public static class WordMapper
            extends Mapper<LongWritable, Text, Text, IntWritable>{

        // Output:
        // We have the hardcoded value in our case which is 1: IntWritable
        private final static IntWritable one = new IntWritable(1);
        // The key is the tokenized words: Text
        private Text word = new Text();

        // Input:
        // We define the data types of input and output key/value pair after the class declaration using angle brackets.
        // Both the input and output of the Mapper is a key/value pair.

        // The key is nothing but the offset of each line in the text file: LongWritable
        // The value is each individual line (as shown in the figure at the right): Text
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            Set<String> str_set = new HashSet<String>();
            while (itr.hasMoreTokens()) {
                String next_token = itr.nextToken();
                if (!str_set.contains(next_token)) {
                    str_set.add(next_token);
                    word.set(next_token);
                    context.write(word, one);
                }
            }
        }
        // We have written a java code where we have tokenized each word
        // and assigned them a hardcoded value equal to 1.
        // Eg: Dear 1, Bear 1,
    }

    public static class WordReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        private Map<String, Integer> counter = new HashMap<>();

        // Input:
        // The key nothing but those unique words which have been generated after the sorting and shuffling phase: Text
        // The value is a list of integers corresponding to each key: IntWritable
        // Eg: Bear, [1, 1],
        // Output:
        // The key is all the unique words present in the input text file: Text
        // The value is the number of occurrences of each of the unique words: IntWritable
        // Eg: Bear, 2; Car, 3,

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            Configuration conf = context.getConfiguration();
            int num_lines = Integer.valueOf(conf.get("num_lines"));
            // TODO: Check edge condition and how to chain output
//            if (num_lines % 2 !=0 && sum > num_lines/2) {
//                counter.put(key.toString(), sum);
//            }
//            else if (num_lines % 2 ==0 && sum >= num_lines/2) {
//                counter.put(key.toString(), sum);
//            }
            if (((double)sum)/num_lines >= 0.25) {
                counter.put(key.toString(), sum);
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            LinkedHashMap<String, Integer> sortedMap = new LinkedHashMap<>();
            counter.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).forEachOrdered(x -> sortedMap.put(x.getKey(), x.getValue()));
            Iterator<Map.Entry<String, Integer>> it = sortedMap.entrySet().iterator();
            System.out.println("OriginalMap: "+ counter);
            System.out.println("SortedMap: "+ sortedMap);
            for (int i=0; i<2; i++) {
                Map.Entry<String, Integer> next = it.next();
                context.write(new Text(next.getKey()), new IntWritable(next.getValue()));
            }
        }
    }


    private static String outputPath;
    private static String inputPath;

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        for(int i = 0; i < args.length; ++i) {
            if (args[i].equals("--input_path")) {
                inputPath = args[++i];
            } else if (args[i].equals("--output_path")) {
                outputPath = args[++i];
            } else {
                throw new IllegalArgumentException("Illegal cmd line arguement");
            }
        }

        if (outputPath == null || inputPath == null) {
            throw new RuntimeException("Either outputpath or input path are not defined");
        }

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        conf.set("mapreduce.job.queuename", "eecs476w21");         // required for this to work on GreatLakes


//        Job wordCountJob = Job.getInstance(conf, "wordCountJob");
//        wordCountJob.setJarByClass(WordCount.class);
//        wordCountJob.setNumReduceTasks(1);
//
//        wordCountJob.setMapperClass(TokenizerMapper.class);
//        wordCountJob.setReducerClass(IntSumReducer.class);
//
//        // set mapper output key and value class
//        // if mapper and reducer output are the same types, you skip
//        wordCountJob.setMapOutputKeyClass(Text.class);
//        wordCountJob.setMapOutputValueClass(LongWritable.class);
//
//        // set reducer output key and value class
//        wordCountJob.setOutputKeyClass(Text.class);
//        wordCountJob.setOutputValueClass(Text.class);
//
//        FileInputFormat.addInputPath(wordCountJob, new Path(inputPath));
//        FileOutputFormat.setOutputPath(wordCountJob, new Path(outputPath));
//
//        wordCountJob.waitForCompletion(true);
        Job wordCountJob = Job.getInstance(conf, "wordCountJob1");
        wordCountJob.setJarByClass(TwoMostFrequentStopWords.class);
        wordCountJob.setNumReduceTasks(1);

        wordCountJob.setMapperClass(CounterMapper.class);
        wordCountJob.setReducerClass(CounterReducer.class);

        // set mapper output key and value class
        // if mapper and reducer output are the same types, you skip
        wordCountJob.setMapOutputKeyClass(IntWritable.class);
        wordCountJob.setMapOutputValueClass(LongWritable.class);

        // set reducer output key and value class
        wordCountJob.setOutputKeyClass(Text.class);
        wordCountJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(wordCountJob, new Path(inputPath));
        System.out.println("outputPath: "+ outputPath);
        FileOutputFormat.setOutputPath(wordCountJob, new Path("out1"));
        wordCountJob.waitForCompletion(true);

        Path newPath = new Path("out1", "part-r-00000");
        FileSystem fs = newPath.getFileSystem(conf);
        FSDataInputStream inputStream = fs.open(newPath);
        BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        String line = bufferedReader.readLine();
        Integer num_lines = Integer.valueOf(line.split(",")[1]);
        System.out.println("Total num of lines: "+num_lines.toString());
        conf.set("num_lines", num_lines.toString());


        wordCountJob = Job.getInstance(conf, "wordCountJob2");
        wordCountJob.setJarByClass(TwoMostFrequentStopWords.class);
        wordCountJob.setNumReduceTasks(1);

        wordCountJob.setMapperClass(WordMapper.class);
        wordCountJob.setReducerClass(WordReducer.class);

        // set mapper output key and value class
        // if mapper and reducer output are the same types, you skip
        wordCountJob.setMapOutputKeyClass(Text.class);
        wordCountJob.setMapOutputValueClass(IntWritable.class);

        // set reducer output key and value class
        wordCountJob.setOutputKeyClass(Text.class);
        wordCountJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(wordCountJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(wordCountJob, new Path(outputPath));
        wordCountJob.waitForCompletion(true);

//        wordCountJob = Job.getInstance(conf, "wordCountJob3");
//        wordCountJob.setJarByClass(StopWords.class);
//        wordCountJob.setNumReduceTasks(1);
//
//        wordCountJob.setMapperClass(DetermineMapper.class);
//        wordCountJob.setReducerClass(DetermineReducer.class);
//
//        // set mapper output key and value class
//        // if mapper and reducer output are the same types, you skip
//        wordCountJob.setMapOutputKeyClass(Text.class);
//        wordCountJob.setMapOutputValueClass(IntWritable.class);
//
//        // set reducer output key and value class
//        wordCountJob.setOutputKeyClass(Text.class);
//        wordCountJob.setOutputValueClass(IntWritable.class);
//
//        FileInputFormat.addInputPath(wordCountJob, new Path("out2"));
//        FileOutputFormat.setOutputPath(wordCountJob, new Path("out3"));
//        wordCountJob.waitForCompletion(true);
    }

}
