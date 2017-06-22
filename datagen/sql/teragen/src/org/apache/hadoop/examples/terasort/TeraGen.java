/**
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

package org.apache.hadoop.examples.terasort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.brown.cs.mapreduce.BenchmarkBase;

/**
 * Generate the official terasort input data set.
 * The user specifies the number of rows and the output directory and this
 * class runs a map/reduce program to generate the data.
 * The format of the data is:
 * <ul>
 * <li>(10 bytes key) (10 bytes rowid) (78 bytes filler) \r \n
 * <li>The keys are random characters from the set ' ' .. '~'.
 * <li>The rowid is the right justified row id as a int.
 * <li>The filler consists of 7 runs of 10 characters from 'A' to 'Z'.
 * </ul>
 *
 * <p>
 * To run the program: 
 * <b>bin/hadoop jar hadoop-*-examples.jar teragen 10000000000 in-dir</b>
 */
public class TeraGen extends Configured implements Tool {
   public final static int KEY_LENGTH = 10;
   public final static int ROWID_LENGTH = 10;
   public final static int VALUE_LENGTH = 80;

  /**
   * An input format that assigns ranges of longs to each mapper.
   */
  static class RangeInputFormat 
       implements InputFormat<LongWritable, NullWritable> {
    
    /**
     * An input split consisting of a range on numbers.
     */
    static class RangeInputSplit implements InputSplit {
      long firstRow;
      long rowCount;

      public RangeInputSplit() { }

      public RangeInputSplit(long offset, long length) {
        firstRow = offset;
        rowCount = length;
      }

      public long getLength() throws IOException {
        return 0;
      }

      public String[] getLocations() throws IOException {
        return new String[]{};
      }

      public void readFields(DataInput in) throws IOException {
        firstRow = WritableUtils.readVLong(in);
        rowCount = WritableUtils.readVLong(in);
      }

      public void write(DataOutput out) throws IOException {
        WritableUtils.writeVLong(out, firstRow);
        WritableUtils.writeVLong(out, rowCount);
      }
    }
    
    /**
     * A record reader that will generate a range of numbers.
     */
    static class RangeRecordReader 
          implements RecordReader<LongWritable, NullWritable> {
      long startRow;
      long finishedRows;
      long totalRows;

      public RangeRecordReader(RangeInputSplit split) {
        startRow = split.firstRow;
        finishedRows = 0;
        totalRows = split.rowCount;
      }

      public void close() throws IOException {
        // NOTHING
      }

      public LongWritable createKey() {
        return new LongWritable();
      }

      public NullWritable createValue() {
        return NullWritable.get();
      }

      public long getPos() throws IOException {
        return finishedRows;
      }

      public float getProgress() throws IOException {
        return finishedRows / (float) totalRows;
      }

      public boolean next(LongWritable key, 
                          NullWritable value) {
        if (finishedRows < totalRows) {
          key.set(startRow + finishedRows);
          finishedRows += 1;
          return true;
        } else {
          return false;
        }
      }
      
    }

    public RecordReader<LongWritable, NullWritable> 
      getRecordReader(InputSplit split, JobConf job,
                      Reporter reporter) throws IOException {
      return new RangeRecordReader((RangeInputSplit) split);
    }

    /**
     * Create the desired number of splits, dividing the number of rows
     * between the mappers.
     */
    public InputSplit[] getSplits(JobConf job, 
                                  int numSplits) {
      long totalRows = getNumberOfRows(job);
      long rowsPerSplit = totalRows / numSplits;
      System.out.println("Generating " + totalRows + " using " + numSplits + 
                         " maps with step of " + rowsPerSplit);

      InputSplit[] splits = new InputSplit[numSplits];
      long currentRow = 0;
      for(int split=0; split < numSplits-1; ++split) {
        splits[split] = new RangeInputSplit(currentRow, rowsPerSplit);
        currentRow += rowsPerSplit;
      }
      splits[numSplits-1] = new RangeInputSplit(currentRow, 
                                                totalRows - currentRow);

      /*InputSplit[] splits = new InputSplit[1];
      long currentRow = 0;
      for(int split=0; split < numSplits-1; ++split) {
         if (split == 48) {
            splits[0] = new RangeInputSplit(currentRow, rowsPerSplit);
         }
         currentRow += rowsPerSplit;
      }
      */
      return splits;
    }

   /* (non-Javadoc)
    * @see org.apache.hadoop.mapred.InputFormat#validateInput(org.apache.hadoop.mapred.JobConf)
    */
   public void validateInput(JobConf job) throws IOException {
      // TODO Auto-generated method stub
      
   }

  }
  
  static long getNumberOfRows(JobConf job) {
    return job.getLong("terasort.num-rows", 0);
  }
  
  static void setNumberOfRows(JobConf job, long numRows) {
    job.setLong("terasort.num-rows", numRows);
  }
  
  static long getSequenceFrequency(JobConf job) {
     return job.getLong("terasort.sequence-freq", 0);
   }
   
   static void setSequenceFrequency(JobConf job, long freq) {
     job.setLong("terasort.sequence-freq", freq);
   }
   
   static String getSequence(JobConf job) {
      return job.getStrings("terasort.sequence", "")[0];
    }
    
    static void setSequence(JobConf job, String seq) {
      job.setStrings("terasort.sequence", seq);
    }

  static class RandomGenerator {
    private long seed = 0;
    private static final long mask32 = (1l<<32) - 1;
    /**
     * The number of iterations separating the precomputed seeds.
     */
    private static final int seedSkip = 128 * 1024 * 1024;
    /**
     * The precomputed seed values after every seedSkip iterations.
     * There should be enough values so that a 2**32 iterations are 
     * covered.
     */
    private static final long[] seeds = new long[]{0L,
                                                   4160749568L,
                                                   4026531840L,
                                                   3892314112L,
                                                   3758096384L,
                                                   3623878656L,
                                                   3489660928L,
                                                   3355443200L,
                                                   3221225472L,
                                                   3087007744L,
                                                   2952790016L,
                                                   2818572288L,
                                                   2684354560L,
                                                   2550136832L,
                                                   2415919104L,
                                                   2281701376L,
                                                   2147483648L,
                                                   2013265920L,
                                                   1879048192L,
                                                   1744830464L,
                                                   1610612736L,
                                                   1476395008L,
                                                   1342177280L,
                                                   1207959552L,
                                                   1073741824L,
                                                   939524096L,
                                                   805306368L,
                                                   671088640L,
                                                   536870912L,
                                                   402653184L,
                                                   268435456L,
                                                   134217728L,
                                                  };

    /**
     * Start the random number generator on the given iteration.
     * @param initalIteration the iteration number to start on
     */
    RandomGenerator(long initalIteration) {
      int baseIndex = (int) ((initalIteration & mask32) / seedSkip);
      seed = seeds[baseIndex];
      for(int i=0; i < initalIteration % seedSkip; ++i) {
        next();
      }
    }

    RandomGenerator() {
      this(0);
    }

    long next() {
      seed = (seed * 3141592621l + 663896637) & mask32;
      return seed;
    }
  }

  /**
   * The Mapper class that given a row number, will generate the appropriate 
   * output line.
   */
  public static class SortGenMapper extends MapReduceBase 
      implements Mapper<LongWritable, NullWritable, Text, Text> {

    protected Text key = new Text();
    protected Text value = new Text();
    protected RandomGenerator rand;
    protected JobConf job;
    protected String sequence;
    protected long sequence_freq = 0;
    protected long sequence_ctr = -1;
    
   @Override
   public void configure(JobConf job) {
      super.configure(job);
      this.job = job;
   }
        
    private String randomString(int length) {
       String ret = "";
       //
       // To speed things up, we'll create the random string in blocks of 5 chars
       //
       long randIdx = 0;
       for (int i = 0; i < length; i++) {
          randIdx = (i % 5 == 0 ? rand.next() / 52 : randIdx / 25);
          ret += (char)(' ' + (randIdx % 95));
       }
       return (ret);
    }
    
    /**
     * Add the rowid to the row.
     * @param rowId
     */
    private String addRowId(long rowId) {
      String rowid = Integer.toString((int) rowId);
      String ret = "";
      int padSpace = ROWID_LENGTH - ret.length();
      for (int i = 0; i < padSpace; i++) {
         ret += "0";
      }
      ret += rowid;
      return (ret);
    }

    /**
     * @param rowId the current row number
     */
    private String addFiller(long rowId) {
       String ret = this.randomString(VALUE_LENGTH);
       if (this.sequence_ctr-- <= 0) {
          //
          // Generate a random position in the remaining part of the value 
          // for where we can put the sequence
          //
          int stop_idx = VALUE_LENGTH - sequence.length();
          int seq_idx = (int)(Math.abs(rand.next()) % stop_idx);
          
          String orig_value = ret;
          ret = orig_value.substring(0, seq_idx);
          ret += sequence;
          ret += orig_value.substring(seq_idx + sequence.length());
            
          this.sequence_ctr = this.sequence_freq;
       }
       return (ret);
    }

    public void map(LongWritable row, NullWritable ignored,
                    OutputCollector<Text, Text> output,
                    Reporter reporter) throws IOException {
      long rowId = row.get();
      if (rand == null) {
        // we use 3 random numbers per a row
        rand = new RandomGenerator(rowId*3);
        this.sequence = getSequence(job);
        this.sequence_freq = getSequenceFrequency(job);
        this.sequence_ctr = this.rand.next() % this.sequence_freq; 
      }
      
      //
      // Random Key
      //
      key.set(this.randomString(KEY_LENGTH).getBytes(), 0, KEY_LENGTH);
      
      //
      // Row ID
      //
      value.clear();
      value.set(this.addRowId(rowId).getBytes(), 0, ROWID_LENGTH);
      
      //
      // Random Value
      //
      value.append(this.addFiller(rowId).getBytes(), 0, VALUE_LENGTH);
      
      //
      // Send it on out!
      //
      output.collect(key, value);
    }

  }

  /**
   * @param args the cli arguments
   */
  public int run(String[] args) throws IOException {
    JobConf job = (JobConf) getConf();
    
    if (args.length != 5) {
       System.err.println("PARAMS: <# of rows> <output path> <sequence> <sequence frequency> <# of maps>");
       System.exit(1);
    }
    
    //
    // Grep Sequence Key
    //
    setSequence(job, args[2]);
    setSequenceFrequency(job, Long.parseLong(args[3]));
    job.setNumMapTasks(Integer.parseInt(args[4]));
    
    setNumberOfRows(job, Long.parseLong(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setJobName("TeraGen");
    job.setJarByClass(TeraGen.class);
    job.setMapperClass(SortGenMapper.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormat(RangeInputFormat.class);
    job.setOutputFormat(TeraOutputFormat.class);
    JobClient.runJob(job);
    return 0;
  }
  
//  public static void main(String[] args) {
//     TeraGen tg = new TeraGen();
//     SortGenMapper m = new SortGenMapper();
//     m.rand = new RandomGenerator(1);
//     m.sequence = "PAVLO";
//     m.sequence_freq = 4;
//     m.sequence_ctr = 0;
//     for (int i = 0; i < 8; i++) {
//        String key = m.randomString(KEY_LENGTH);
//        String value = m.addRowId(i);
//        value += m.addFiller(i);
//        System.out.println(i + ": " + key + value);
//     }
//     
//  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new JobConf(), new TeraGen(), args);
    System.exit(res);
  }

}
