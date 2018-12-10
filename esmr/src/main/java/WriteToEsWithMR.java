import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WriteToEsWithMR extends Configured implements Tool {

    public static class EsMapper extends Mapper<Object, Text, NullWritable, Text> {
        private Text doc = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (value.getLength() > 0) {
                doc.set(value);
                System.out.println(value);
                context.write(NullWritable.get(), doc);
            }
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        conf.setBoolean("mapreduce.map.speculative", false);
        conf.setBoolean("mapreduce.reduce.speculative", false);
        conf.set("es.nodes", "10.0.4.17:9200");
        conf.set("es.nodes.wan.only", "true");
        conf.set("es.resource", "logs-201998/type");
        conf.set("es.input.json", "yes");

        Job job = Job.getInstance(conf);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(EsOutputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setJarByClass(WriteToEsWithMR.class);
        job.setMapperClass(EsMapper.class);

        FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new WriteToEsWithMR(), args);
        System.exit(ret);
    }
}

