import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Cette classe effectue une réduction partielle à la sortie des Mappers sur une même machine
 * Elle lit une liste de paires ("V", (n, Σx, Σx²)) et émet une paire ("V", (n, Σx, Σx²))
 */
public class RegressionCombiner
        extends Reducer<Text, Regression, Text, Regression>
{
    /** traite une liste de paires produites par les mappers de cette machine */
    @Override
    public void reduce(Text cleI, Iterable<Regression> valeursI, Context context)
            throws IOException, InterruptedException
    {
        // définir la clé de sortie
        Text cleS = cleI;

        // calculer la valeur de sortie
        Regression valeurS = new Regression();
        for (Regression valeurI : valeursI) {
            valeurS.add(valeurI);
        }
        
        
        

        // émettre une paire (clé, valeur)
        context.write(cleS, valeurS);
    }
}
