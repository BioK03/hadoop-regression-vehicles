import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Cette classe émet une paire ("V", (1, x, x²)) par ligne de texte reçue, x=longueur de la ligne
 * @note la clé "V" n'a pas d'importance, il faut juste que ça soit la même pour toutes les paires
 */
public class RegressionMapper
        extends Mapper<LongWritable, Text, Text, Regression>
{
    /* paire (clé, valeur) émise par ce Mapper */
    private Text cleI = new Text("V");
    private Regression valeurI = new Regression();

    /** traite l'une des lignes du fichier */
    @Override
    public void map(LongWritable cleE, Text valeurE, Context context)
            throws IOException, InterruptedException
    {
        try {
            // calculer la valeur de sortie
        	
        	int weight = Integer.parseInt(extractData(valeurE, 0).toString());
        	double consumption = Double.parseDouble(extractData(valeurE, 1).toString());
        	
        	
        	valeurI.set(weight, consumption);
        	
        	context.write(cleI, valeurI);
            

        } catch (Exception e) {
            // rien, on ignore cet arbre
        }
   }
    
    protected Text extractData(Text input, int column){
		String sInput = input.toString();
		String[] columns = sInput.split(";");
		return new Text(columns[column]);
	}
    
    protected double ageCalulation(double year){
    	return 2017d-year;
    }
	
}
