import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Programme principal de lancement du calcul de variance
 */
public class RegressionDriver
    extends Configured
    implements Tool
{
    @Override
    public int run(String[] args) throws Exception
    {
        // vérifier les paramètres
        if (args.length != 2) {
            System.err.println("Usage: VarianceDriver <input path> <outputpath>");
            System.exit(-1);
        }

        // créer le job map-reduce
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Variance Job");
        job.setJarByClass(RegressionDriver.class);

        // définir les classes Mapper, Combiner et Reducer
        job.setMapperClass(RegressionMapper.class);
        job.setCombinerClass(RegressionCombiner.class);
        job.setReducerClass(RegressionReducer.class);

        // définir les données d'entrée : TextInputFormat => clés=LongWritable, valeurs=Text
        FileInputFormat.addInputPath(job, new Path(args[0]));   /** VOIR LE MAKEFILE **/
        job.setInputFormatClass(TextInputFormat.class);

        // sorties du mapper = entrées du reducer et du combiner
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Regression.class);

        // définir les données de sortie : dossier et types des clés et valeurs
        FileOutputFormat.setOutputPath(job, new Path(args[1])); /** VOIR LE MAKEFILE **/
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // lancer le job et attendre sa fin
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    /**
     * point d'entrée du programme
     * @param args contient deux éléments : dossier à traiter et dossier des résultats
     * @throws Exception
     */
    public static void main(String[] args) throws Exception
    {
        // préparer et lancer un job
        RegressionDriver driver = new RegressionDriver();
        int exitCode = ToolRunner.run(driver, args);
        System.exit(exitCode);
    }
}
